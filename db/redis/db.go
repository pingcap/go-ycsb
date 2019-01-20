package redis

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"strings"
	"time"

	goredis "github.com/go-redis/redis"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

func init() {
	ycsb.RegisterDBCreator("redis", redisCreator{})
}

type redis struct {
	singleCli  *goredis.Client
	clusterCli *goredis.ClusterClient
	mode       string
}

func (r *redis) Close() error {
	var err error
	switch r.mode {
	case "cluster":
		err = r.clusterCli.Close()
	case "single":
		fallthrough
	default:
		err = r.singleCli.Close()
	}
	return err
}

func (r *redis) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *redis) CleanupThread(_ context.Context) {
}

func (r *redis) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	var res string
	var err error

	data := map[string][]byte{}

	switch r.mode {
	case "cluster":
		res, err = r.clusterCli.Get(table + "/" + key).Result()
		if err != nil {
			return nil, err
		}
	case "single":
		fallthrough
	default:
		res, err = r.singleCli.Get(table + "/" + key).Result()
		if err != nil {
			return nil, err
		}
	}

	err = json.Unmarshal([]byte(res), &data)
	if err != nil {
		return nil, err
	}

	return data, err
}

var cursor uint64

func (r *redis) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	data := []map[string][]byte{}
	var keys []string
	var err error

	switch r.mode {
	case "cluster":
		keys, cursor, err = r.clusterCli.Scan(cursor, table+"/*", int64(count)).Result()
		if err != nil {
			return nil, err
		}

		for _, k := range keys {
			res, err := r.clusterCli.Get(k).Result()
			if err != nil {
				return nil, err
			}

			tmp := map[string][]byte{}
			err = json.Unmarshal([]byte(res), &tmp)
			if err != nil {
				return nil, err
			}

			data = append(data, tmp)
		}
	case "single":
		fallthrough
	default:
		keys, cursor, err = r.singleCli.Scan(cursor, table+"/*", int64(count)).Result()
		if err != nil {
			return nil, err
		}

		for _, k := range keys {
			res, err := r.singleCli.Get(k).Result()
			if err != nil {
				return nil, err
			}

			tmp := map[string][]byte{}
			err = json.Unmarshal([]byte(res), &tmp)
			if err != nil {
				return nil, err
			}

			data = append(data, tmp)
		}
	}

	return data, nil
}

func (r *redis) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	switch r.mode {
	case "cluster":
		err = r.clusterCli.Set(table+"/"+key, string(data), 0).Err()
	case "single":
		fallthrough
	default:
		err = r.singleCli.Set(table+"/"+key, string(data), 0).Err()
	}

	return err
}

func (r *redis) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	switch r.mode {
	case "cluster":
		err = r.clusterCli.Set(table+"/"+key, string(data), 0).Err()
	case "single":
		fallthrough
	default:
		err = r.singleCli.Set(table+"/"+key, string(data), 0).Err()
	}

	return err
}

func (r *redis) Delete(ctx context.Context, table string, key string) error {
	var err error

	switch r.mode {
	case "cluster":
		err = r.clusterCli.Del(table + "/" + key).Err()
	case "single":
		fallthrough
	default:
		err = r.singleCli.Del(table + "/" + key).Err()
	}

	return err
}

type redisCreator struct{}

func (r redisCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rds := &redis{}

	mode, _ := p.Get(redisMode)
	switch mode {
	case "cluster":
		rds.clusterCli = goredis.NewClusterClient(getOptionsCluster(p))

		if p.GetBool(prop.DropData, prop.DropDataDefault) {
			err := rds.clusterCli.FlushDB().Err()
			if err != nil {
				return nil, err
			}
		}
	case "single":
		fallthrough
	default:
		mode = "single"
		rds.singleCli = goredis.NewClient(getOptionsSingle(p))

		if p.GetBool(prop.DropData, prop.DropDataDefault) {
			err := rds.singleCli.FlushDB().Err()
			if err != nil {
				return nil, err
			}
		}
	}
	rds.mode = mode

	return rds, nil
}

const (
	redisMode                  = "redis.mode"
	redisNetwork               = "redis.network"
	redisAddr                  = "redis.addr"
	redisPassword              = "redis.password"
	redisDB                    = "redis.db"
	redisMaxRedirects          = "redis.max_redirects"
	redisReadOnly              = "redis.read_only"
	redisRouteByLatency        = "redis.route_by_latency"
	redisRouteRandomly         = "redis.route_randomly"
	redisMaxRetries            = "redis.max_retries"
	redisMinRetryBackoff       = "redis.min_retry_backoff"
	redisMaxRetryBackoff       = "redis.max_retry_backoff"
	redisDialTimeout           = "redis.dial_timeout"
	redisReadTimeout           = "redis.read_timeout"
	redisWriteTimeout          = "redis.write_timeout"
	redisPoolSize              = "redis.pool_size"
	redisMinIdleConns          = "redis.min_idle_conns"
	redisMaxConnAge            = "redis.max_conn_age"
	redisPoolTimeout           = "redis.pool_timeout"
	redisIdleTimeout           = "redis.idle_timeout"
	redisIdleCheckFreq         = "redis.idle_check_frequency"
	redisTLSCA                 = "redis.tls_ca"
	redisTLSCert               = "redis.tls_cert"
	redisTLSKey                = "redis.tls_key"
	redisTLSInsecureSkipVerify = "redis.tls_insecure_skip_verify"
)

func getOptionsSingle(p *properties.Properties) *goredis.Options {
	opts := &goredis.Options{}
	opts.Network, _ = p.Get(redisNetwork)
	opts.Addr, _ = p.Get(redisAddr)
	opts.Password, _ = p.Get(redisPassword)
	opts.DB = p.GetInt(redisDB, 0)
	opts.MaxRetries = p.GetInt(redisMaxRetries, 0)
	opts.MinRetryBackoff = p.GetDuration(redisMinRetryBackoff, time.Millisecond*8)
	opts.MaxRetryBackoff = p.GetDuration(redisMaxRetryBackoff, time.Millisecond*512)
	opts.DialTimeout = p.GetDuration(redisDialTimeout, time.Second*5)
	opts.ReadTimeout = p.GetDuration(redisReadTimeout, time.Second*3)
	opts.WriteTimeout = p.GetDuration(redisWriteTimeout, opts.ReadTimeout)
	opts.PoolSize = p.GetInt(redisPoolSize, 10)
	opts.MinIdleConns = p.GetInt(redisMinIdleConns, 0)
	opts.MaxConnAge = p.GetDuration(redisMaxConnAge, 0)
	opts.PoolTimeout = p.GetDuration(redisPoolTimeout, time.Second+opts.ReadTimeout)
	opts.IdleTimeout = p.GetDuration(redisIdleTimeout, time.Minute*5)
	opts.IdleCheckFrequency = p.GetDuration(redisIdleCheckFreq, time.Minute)

	var err error
	caPath, _ := p.Get(redisTLSCA)
	certPath, _ := p.Get(redisTLSCert)
	keyPath, _ := p.Get(redisTLSKey)
	insecureSkipVerify := p.GetBool(redisTLSInsecureSkipVerify, false)
	if certPath != "" && keyPath != "" {
		opts.TLSConfig, err = getTLS(caPath, certPath, keyPath, insecureSkipVerify)
		if err != nil {
			opts.TLSConfig = nil
		}
	}

	return opts
}

func getOptionsCluster(p *properties.Properties) *goredis.ClusterOptions {
	opts := &goredis.ClusterOptions{}

	addresses, _ := p.Get(redisAddr)
	opts.Addrs = strings.Split(addresses, ";")
	opts.MaxRedirects = p.GetInt(redisMaxRedirects, 0)
	opts.ReadOnly = p.GetBool(redisReadOnly, false)
	opts.RouteByLatency = p.GetBool(redisRouteByLatency, false)
	opts.RouteRandomly = p.GetBool(redisRouteRandomly, false)
	opts.Password, _ = p.Get(redisPassword)
	opts.MaxRetries = p.GetInt(redisMaxRetries, 0)
	opts.MinRetryBackoff = p.GetDuration(redisMinRetryBackoff, time.Millisecond*8)
	opts.MaxRetryBackoff = p.GetDuration(redisMaxRetryBackoff, time.Millisecond*512)
	opts.DialTimeout = p.GetDuration(redisDialTimeout, time.Second*5)
	opts.ReadTimeout = p.GetDuration(redisReadTimeout, time.Second*3)
	opts.WriteTimeout = p.GetDuration(redisWriteTimeout, opts.ReadTimeout)
	opts.PoolSize = p.GetInt(redisPoolSize, 10)
	opts.MinIdleConns = p.GetInt(redisMinIdleConns, 0)
	opts.MaxConnAge = p.GetDuration(redisMaxConnAge, 0)
	opts.PoolTimeout = p.GetDuration(redisPoolTimeout, time.Second+opts.ReadTimeout)
	opts.IdleTimeout = p.GetDuration(redisIdleTimeout, time.Minute*5)
	opts.IdleCheckFrequency = p.GetDuration(redisIdleCheckFreq, time.Minute)

	var err error
	caPath, _ := p.Get(redisTLSCA)
	certPath, _ := p.Get(redisTLSCert)
	keyPath, _ := p.Get(redisTLSKey)
	insecureSkipVerify := p.GetBool(redisTLSInsecureSkipVerify, false)
	if certPath != "" && keyPath != "" {
		opts.TLSConfig, err = getTLS(caPath, certPath, keyPath, insecureSkipVerify)
		if err != nil {
			opts.TLSConfig = nil
		}
	}

	return opts
}

func getTLS(caPath, certPath, keyPath string, insecureSkipVerify bool) (*tls.Config, error) {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: insecureSkipVerify,
		Renegotiation:      tls.RenegotiateNever,
	}

	if caPath != "" {
		pool, err := makeCertPool([]string{caPath})
		if err != nil {
			return nil, err
		}
		tlsConfig.RootCAs = pool
	}

	if certPath != "" && keyPath != "" {
		err := loadCertificate(tlsConfig, certPath, keyPath)
		if err != nil {
			return nil, err
		}
	}

	return tlsConfig, nil
}

func makeCertPool(certFiles []string) (*x509.CertPool, error) {
	pool := x509.NewCertPool()
	for _, certFile := range certFiles {
		pem, err := ioutil.ReadFile(certFile)
		if err != nil {
			return nil, fmt.Errorf("could not read certificate %q: %v", certFile, err)
		}
		ok := pool.AppendCertsFromPEM(pem)
		if !ok {
			return nil, fmt.Errorf("could not parse any PEM certificates %q: %v", certFile, err)
		}
	}
	return pool, nil
}

func loadCertificate(config *tls.Config, certFile, keyFile string) error {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return fmt.Errorf("could not load keypair %s:%s: %v", certFile, keyFile, err)
	}

	config.Certificates = []tls.Certificate{cert}
	config.BuildNameToCertificate()
	return nil
}
