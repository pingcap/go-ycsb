package ydb

import (
	"context"
	"net/http"
	_ "net/http/pprof"

	"github.com/magiconair/properties"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type creator struct{}

var (
	_ ycsb.DBCreator = (*creator)(nil)
)

func (c creator) Create(p *properties.Properties) (_ ycsb.DB, err error) {
	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			panic(err)
		}
	}()

	dsn := p.GetString(ydbDSN, ydbDSNDefault)

	ctx := context.Background()

	threadCount := int(p.GetInt64(prop.ThreadCount, prop.ThreadCountDefault))

	d := &driver{
		p:           p,
		verbose:     p.GetBool(prop.Verbose, prop.VerboseDefault),
		forceUpsert: p.GetBool(ydbForceUpsert, ydbForceUpsertDefault),
	}

	if p.GetBool(ydbNativeDriver, ydbNativeDriverDefault) {
		d.core, err = openNative(ctx, dsn, threadCount)
	} else {
		d.core, err = openSql(ctx, dsn, threadCount)
	}

	if err != nil {
		return nil, err
	}

	return d, d.createTable()
}
