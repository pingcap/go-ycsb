package redis

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	goredis "github.com/go-redis/redis"
	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type redisClient interface {
	Get(key string) *goredis.StringCmd
	Scan(cursor uint64, match string, count int64) *goredis.ScanCmd
	Set(key string, value interface{}, expiration time.Duration) *goredis.StatusCmd
	Del(keys ...string) *goredis.IntCmd
	FlushDB() *goredis.StatusCmd
	Close() error
}

type redis struct {
	client redisClient
	mode   string
}

func (db *redis) ToSqlDB() *sql.DB {
	return nil
}

func (r *redis) Close() error {
	return r.client.Close()
}

func (r *redis) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *redis) CleanupThread(_ context.Context) {
}

func (r *redis) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	data := make(map[string][]byte, len(fields))

	res, err := r.client.Get(table + "/" + key).Result()

	if err != nil {
		return nil, err
	}

	err = json.Unmarshal([]byte(res), &data)
	if err != nil {
		return nil, err
	}

	// TODO: filter by fields

	return data, err
}

func (r *redis) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (r *redis) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	d, err := r.client.Get(table + "/" + key).Result()
	if err != nil {
		return err
	}

	curVal := map[string][]byte{}
	err = json.Unmarshal([]byte(d), &curVal)
	if err != nil {
		return err
	}
	for k, v := range values {
		curVal[k] = v
	}
	var data []byte
	data, err = json.Marshal(curVal)
	if err != nil {
		return err
	}

	return r.client.Set(table+"/"+key, string(data), 0).Err()
}

func (r *redis) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	data, err := json.Marshal(values)
	if err != nil {
		return err
	}

	return r.client.Set(table+"/"+key, string(data), 0).Err()
}

func (r *redis) Delete(ctx context.Context, table string, key string) error {
	return r.client.Del(table + "/" + key).Err()
}

type redisCreator struct{}

func (r redisCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rds := &redis{}

	mode, _ := p.Get(redisMode)
	switch mode {
	case "cluster":
		rds.client = goredis.NewClusterClient(getOptionsCluster(p))

		if p.GetBool(prop.DropData, prop.DropDataDefault) {
			err := rds.client.FlushDB().Err()
			if err != nil {
				return nil, err
			}
		}
	case "single":
		fallthrough
	default:
		mode = "single"
		rds.client = goredis.NewClient(getOptionsSingle(p))

		if p.GetBool(prop.DropData, prop.DropDataDefault) {
			err := rds.client.FlushDB().Err()
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

func parseTLS(p *properties.Properties) *tls.Config {
	caPath, _ := p.Get(redisTLSCA)
	certPath, _ := p.Get(redisTLSCert)
	keyPath, _ := p.Get(redisTLSKey)
	insecureSkipVerify := p.GetBool(redisTLSInsecureSkipVerify, false)
	if certPath != "" && keyPath != "" {
		config, err := util.CreateTLSConfig(caPath, certPath, keyPath, insecureSkipVerify)
		if err == nil {
			return config
		}
	}

	return nil
}

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

	opts.TLSConfig = parseTLS(p)

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

	opts.TLSConfig = parseTLS(p)

	return opts
}

func init() {
	ycsb.RegisterDBCreator("redis", redisCreator{})
}
