package s3

import (
	"bytes"
	"context"
	"errors"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

// S3 property keys and default values.
const (
	// S3 bucket name
	s3Bucket    = "s3.bucket"
	s3BucketDef = "ycsb"

	// S3 region
	s3Region    = "s3.region"
	s3RegionDef = "us-east-1"

	// S3 endpoint
	s3Endpoint    = "s3.endpoint"
	s3EndpointDef = "" // empty means use AWS default

	// S3 access key
	s3AccessKey = "s3.access_key"

	// S3 secret key
	s3SecretKey = "s3.secret_key"

	// S3 use path style
	s3UsePathStyle    = "s3.use_path_style"
	s3UsePathStyleDef = false

	// S3 update overwrite
	s3UpdateOverwrite    = "s3.update_overwrite"
	s3UpdateOverwriteDef = true

	// S3 scan keys only
	s3ScanKeysOnly    = "s3.scan_keys_only"
	s3ScanKeysOnlyDef = false

	// S3 client pool size (number of s3 clients distributed equally to the benchmarking threads)
	s3ClientPoolSize    = "s3.client_pool_size"
	s3ClientPoolSizeDef = 1
)

// s3Creator implements ycsb.DBCreator for the S3 backend.
type s3Creator struct{}

// Create instantiates the S3 driver, parsing configuration properties and launching an AWS SDK v2 S3 client.
func (s s3Creator) Create(p *properties.Properties) (ycsb.DB, error) {
	bucket := p.GetString(s3Bucket, s3BucketDef)
	region := p.GetString(s3Region, s3RegionDef)
	endpoint := p.GetString(s3Endpoint, s3EndpointDef)
	accessKey := p.GetString(s3AccessKey, "")
	secretKey := p.GetString(s3SecretKey, "")
	usePathStyle := p.GetBool(s3UsePathStyle, s3UsePathStyleDef)
	updateOverwrite := p.GetBool(s3UpdateOverwrite, s3UpdateOverwriteDef)
	scanKeysOnly := p.GetBool(s3ScanKeysOnly, s3ScanKeysOnlyDef)
	clientPoolSize := p.GetInt(s3ClientPoolSize, s3ClientPoolSizeDef)

	if clientPoolSize < 1 {
		clientPoolSize = 1
	}

	// Assemble AWS SDK loading options.
	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
	}

	if accessKey != "" && secretKey != "" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKey, secretKey, "")))
	}

	if endpoint != "" {
		// Custom endpoint resolver (for S3-compatible services)
		loadOpts = append(loadOpts, config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, resRegion string) (aws.Endpoint, error) {
				if service == awss3.ServiceID {
					return aws.Endpoint{URL: endpoint, SigningRegion: resRegion}, nil
				}
				return aws.Endpoint{}, &aws.EndpointNotFoundError{}
			})))
	}

	cfg, err := config.LoadDefaultConfig(context.TODO(), loadOpts...)
	if err != nil {
		return nil, err
	}

	// Create multiple S3 clients for the pool
	clients := make([]*awss3.Client, clientPoolSize)
	for i := 0; i < clientPoolSize; i++ {
		clients[i] = awss3.NewFromConfig(cfg, func(o *awss3.Options) {
			o.UsePathStyle = usePathStyle
		})
	}

	// Ensure bucket exists (best-effort) using the first client.
	ctx := context.TODO()
	_, err = clients[0].HeadBucket(ctx, &awss3.HeadBucketInput{Bucket: &bucket})
	if err != nil {
		// Attempt to create bucket if not found.
		_, cErr := clients[0].CreateBucket(ctx, &awss3.CreateBucketInput{
			Bucket: &bucket,
		})
		if cErr != nil {
			// Ignore if bucket already exists in another region or owned by you.
			var bExists *s3types.BucketAlreadyOwnedByYou
			var bExists2 *s3types.BucketAlreadyExists
			if !errors.As(cErr, &bExists) && !errors.As(cErr, &bExists2) {
				return nil, cErr
			}
		}
	}

	return &s3DB{
		clients:         clients,
		bucket:          bucket,
		updateOverwrite: updateOverwrite,
		scanKeysOnly:    scanKeysOnly,
	}, nil
}

// s3DB implements the ycsb.DB interface for S3-compatible services.
type s3DB struct {
	clients []*awss3.Client
	bucket  string

	// if true, update will overwrite existing object
	// otherwise, update will perform a read-modify-write operation
	updateOverwrite bool

	// if true, scan will return only the keys of the objects
	scanKeysOnly bool
}

// contextKey is a type for context keys to avoid collisions
type contextKey string

const clientIndexKey contextKey = "s3ClientIndex"

// Close closes the driver. No-op for now.
func (db *s3DB) Close() error { return nil }

// InitThread initializes any per-worker state and assigns a client to this thread.
// The assignment distributes threads uniformly across the client pool.
func (db *s3DB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	clientPoolSize := len(db.clients)

	// If we have as many or more clients than threads, each thread gets its own client
	if clientPoolSize >= threadCount {
		return context.WithValue(ctx, clientIndexKey, threadID%clientPoolSize)
	}

	// Otherwise, distribute threads uniformly across clients
	// This ensures the most even distribution possible
	clientIndex := (threadID * clientPoolSize) / threadCount
	return context.WithValue(ctx, clientIndexKey, clientIndex)
}

// CleanupThread cleans up per-worker state. No-op.
func (db *s3DB) CleanupThread(ctx context.Context) {}

// getClient retrieves the S3 client assigned to the current thread from context.
// If no client is assigned (shouldn't happen in normal operation), it defaults to the first client.
func (db *s3DB) getClient(ctx context.Context) *awss3.Client {
	if idx, ok := ctx.Value(clientIndexKey).(int); ok && idx < len(db.clients) {
		return db.clients[idx]
	}
	// Fallback to first client if context doesn't have a client index
	return db.clients[0]
}

// Read fetches a record by key.
func (db *s3DB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	objectKey := db.composeObjectKey(table, key)

	client := db.getClient(ctx)
	out, err := client.GetObject(ctx, &awss3.GetObjectInput{
		Bucket: &db.bucket,
		Key:    &objectKey,
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()
	payload, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, err
	}

	allValues, err := decodeValues(payload)
	if err != nil {
		return nil, err
	}

	// If specific fields requested, filter.
	if len(fields) > 0 {
		filtered := make(map[string][]byte, len(fields))
		for _, f := range fields {
			if v, ok := allValues[f]; ok {
				filtered[f] = v
			}
		}
		return filtered, nil
	}

	return allValues, nil
}

// Scan iterates over a range of records.
func (db *s3DB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	if count <= 0 {
		return nil, nil
	}

	results := make([]map[string][]byte, 0, count)

	// Include the startKey itself if it exists.
	if startKey != "" {
		if db.scanKeysOnly {
			rec := map[string][]byte{startKey: nil}
			results = append(results, rec)
		} else {
			rec, err := db.Read(ctx, table, startKey, fields)
			if err == nil && rec != nil {
				results = append(results, rec)
			}
		}

		if len(results) >= count {
			return results, nil // return early if we have enough results
		}
	}

	prefix := ""
	if table != "" {
		prefix = table + "/"
	}

	startAfter := db.composeObjectKey(table, startKey)

	var contToken *string
	remaining := count - len(results)

	client := db.getClient(ctx)
	for remaining > 0 {
		maxKeys := int32(remaining)
		if maxKeys > 1000 {
			maxKeys = 1000
		}

		out, err := client.ListObjectsV2(ctx, &awss3.ListObjectsV2Input{
			Bucket:            &db.bucket,
			Prefix:            &prefix,
			StartAfter:        &startAfter,
			ContinuationToken: contToken,
			MaxKeys:           maxKeys,
		})
		if err != nil {
			return nil, err
		}

		if len(out.Contents) == 0 {
			break
		}

		for _, obj := range out.Contents {
			if remaining == 0 {
				break
			}
			key := *obj.Key
			// Skip the startKey if it reappears
			if key == startAfter {
				continue
			}

			// Extract record key without table prefix.
			recordKey := key
			if prefix != "" && len(key) > len(prefix) && key[:len(prefix)] == prefix {
				recordKey = key[len(prefix):]
			}

			rec := map[string][]byte{recordKey: nil}
			// if scanKeysOnly is true, we don't need to read the object
			if !db.scanKeysOnly {
				rec, err = db.Read(ctx, table, recordKey, fields)
				if err != nil {
					// skip problematic record
					continue
				}
			}
			results = append(results, rec)
			remaining--
		}

		if !out.IsTruncated || remaining == 0 {
			break
		}
		contToken = out.NextContinuationToken
	}

	return results, nil
}

// Update modifies an existing record.
func (db *s3DB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	if !db.updateOverwrite {
		// read-modify-write operation
		existing, _ := db.Read(ctx, table, key, nil)
		if existing == nil {
			existing = make(map[string][]byte)
		}
		for k, v := range values {
			existing[k] = v
		}
		return db.Insert(ctx, table, key, existing)
	}

	// overwrite existing object
	return db.Insert(ctx, table, key, values)
}

// Insert inserts a new record.
func (db *s3DB) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	objectKey := db.composeObjectKey(table, key)

	payload, err := encodeValues(values)
	if err != nil {
		return err
	}

	client := db.getClient(ctx)
	_, err = client.PutObject(ctx, &awss3.PutObjectInput{
		Bucket: &db.bucket,
		Key:    &objectKey,
		Body:   bytes.NewReader(payload),
	})
	return err
}

// Delete removes a record.
func (db *s3DB) Delete(ctx context.Context, table string, key string) error {
	objectKey := db.composeObjectKey(table, key)
	client := db.getClient(ctx)
	_, err := client.DeleteObject(ctx, &awss3.DeleteObjectInput{
		Bucket: &db.bucket,
		Key:    &objectKey,
	})
	return err
}

// composeObjectKey builds an S3 object key using table as prefix.
func (db *s3DB) composeObjectKey(table, key string) string {
	if table == "" {
		return key
	}
	return table + "/" + key
}

// --- Batch operations: simple per-key loops for now ---

// BatchInsert inserts multiple records.
func (db *s3DB) BatchInsert(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	for i, k := range keys {
		if err := db.Insert(ctx, table, k, values[i]); err != nil {
			return err
		}
	}
	return nil
}

// BatchRead reads multiple records.
func (db *s3DB) BatchRead(ctx context.Context, table string, keys []string, fields []string) ([]map[string][]byte, error) {
	out := make([]map[string][]byte, len(keys))
	for i, k := range keys {
		rec, err := db.Read(ctx, table, k, fields)
		if err != nil {
			return nil, err
		}
		out[i] = rec
	}
	return out, nil
}

// BatchUpdate updates multiple records.
func (db *s3DB) BatchUpdate(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	for i, k := range keys {
		if err := db.Update(ctx, table, k, values[i]); err != nil {
			return err
		}
	}
	return nil
}

// BatchDelete deletes multiple records.
func (db *s3DB) BatchDelete(ctx context.Context, table string, keys []string) error {
	for _, k := range keys {
		if err := db.Delete(ctx, table, k); err != nil {
			return err
		}
	}
	return nil
}

// init registers the S3 database creator.
func init() {
	ycsb.RegisterDBCreator("s3", s3Creator{})
}
