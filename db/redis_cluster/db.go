package redis_cluster

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
	ycsb.RegisterDBCreator("redis_cluster", redisClusterCreator{})
}

type redisCluster struct {
	cli *goredis.ClusterClient
}

func (r *redisCluster) Close() error {
	return r.cli.Close()
}

func (r *redisCluster) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *redisCluster) CleanupThread(_ context.Context) {
}

func (r *redisCluster) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	data := map[string][]byte{}
	res, err := r.cli.Get(table + "/" + key).Result()
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(res), &data)
	if err != nil {
		return nil, err
	}

	return data, err
}

var cursor uint64

func (r *redisCluster) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	data := []map[string][]byte{}
	var keys []string
	var err error

	keys, cursor, err = r.cli.Scan(cursor, table+"/*", int64(count)).Result()
	if err != nil {
		return nil, err
	}

	for _, k := range keys {
		res, err := r.cli.Get(k).Result()
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

	return data, nil
}

func (r *redisCluster) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return r.cli.Set(table+"/"+key, string(data), 0).Err()
}

func (r *redisCluster) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return r.cli.Set(table+"/"+key, string(data), 0).Err()
}

func (r *redisCluster) Delete(ctx context.Context, table string, key string) error {
	return r.cli.Del(table + "/" + key).Err()
}

type redisClusterCreator struct{}

func (r redisClusterCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	cli := goredis.NewClusterClient(getOptions(p))

	if p.GetBool(prop.DropData, prop.DropDataDefault) {
		err := cli.FlushDB().Err()
		if err != nil {
			return nil, err
		}
	}

	return &redisCluster{
		cli: cli,
	}, nil
}

const (
	redisAddrs                 = "redis_cluster.addrs"
	redisMaxRedirects          = "redis_cluster.max_redirects"
	redisReadOnly              = "redis_cluster.read_only"
	redisRouteByLatency        = "redis_cluster.route_by_latency"
	redisRouteRandomly         = "redis_cluster.route_randomly"
	redisPassword              = "redis_cluster.password"
	redisMaxRetries            = "redis_cluster.max_retries"
	redisMinRetryBackoff       = "redis_cluster.min_retry_backoff"
	redisMaxRetryBackoff       = "redis_cluster.max_retry_backoff"
	redisDialTimeout           = "redis_cluster.dial_timeout"
	redisReadTimeout           = "redis_cluster.read_timeout"
	redisWriteTimeout          = "redis_cluster.write_timeout"
	redisPoolSize              = "redis_cluster.pool_size"
	redisMinIdleConns          = "redis_cluster.min_idle_conns"
	redisMaxConnAge            = "redis_cluster.max_conn_age"
	redisPoolTimeout           = "redis_cluster.pool_timeout"
	redisIdleTimeout           = "redis_cluster.idle_timeout"
	redisIdleCheckFreq         = "redis_cluster.idle_check_frequency"
	redisTLSCA                 = "redis_cluster.tls_ca"
	redisTLSCert               = "redis_cluster.tls_cert"
	redisTLSKey                = "redis_cluster.tls_key"
	redisTLSInsecureSkipVerify = "redis_cluster.tls_insecure_skip_verify"
)

func getOptions(p *properties.Properties) *goredis.ClusterOptions {
	opts := &goredis.ClusterOptions{}

	addresses, _ := p.Get(redisAddrs)
	opts.Addrs = strings.Split(addresses, ",")

	opts.MaxRedirects = p.GetInt(redisMaxRedirects, 0)
	opts.ReadOnly = p.GetBool(redisReadOnly, false)
	opts.RouteByLatency = p.GetBool(redisRouteByLatency, false)
	opts.RouteRandomly = p.GetBool(redisRouteRandomly, false)

	opts.Password, _ = p.Get(redisPassword)
	opts.MaxRetries = p.GetInt(redisMaxRetries, 0)

	var err error
	tmp, ok := p.Get(redisMinRetryBackoff)
	if ok {
		opts.MinRetryBackoff, err = time.ParseDuration(tmp)
		if err != nil {
			opts.MinRetryBackoff = 0
		}
	}

	tmp, ok = p.Get(redisMaxRetryBackoff)
	if ok {
		opts.MaxRetryBackoff, err = time.ParseDuration(tmp)
		if err != nil {
			opts.MaxRetryBackoff = 0
		}
	}

	tmp, ok = p.Get(redisDialTimeout)
	if ok {
		opts.DialTimeout, err = time.ParseDuration(tmp)
		if err != nil {
			opts.DialTimeout = 0
		}
	}

	tmp, ok = p.Get(redisReadTimeout)
	if ok {
		opts.ReadTimeout, err = time.ParseDuration(tmp)
		if err != nil {
			opts.ReadTimeout = 0
		}
	}

	tmp, ok = p.Get(redisWriteTimeout)
	if ok {
		opts.WriteTimeout, err = time.ParseDuration(tmp)
		if err != nil {
			opts.WriteTimeout = 0
		}
	}

	opts.PoolSize = p.GetInt(redisPoolSize, 10)
	opts.MinIdleConns = p.GetInt(redisMinIdleConns, 0)

	tmp, ok = p.Get(redisMaxConnAge)
	if ok {
		opts.MaxConnAge, err = time.ParseDuration(tmp)
		if err != nil {
			opts.MaxConnAge = 0
		}
	}

	tmp, ok = p.Get(redisPoolTimeout)
	if ok {
		opts.PoolTimeout, err = time.ParseDuration(tmp)
		if err != nil {
			opts.PoolTimeout = 0
		}
	}

	tmp, ok = p.Get(redisIdleTimeout)
	if ok {
		opts.IdleTimeout, err = time.ParseDuration(tmp)
		if err != nil {
			opts.IdleTimeout = 0
		}
	}

	tmp, ok = p.Get(redisIdleCheckFreq)
	if ok {
		opts.IdleCheckFrequency, err = time.ParseDuration(tmp)
		if err != nil {
			opts.IdleCheckFrequency = 0
		}
	}

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
