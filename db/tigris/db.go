package tigris

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/tigrisdata/tigris-client-go/config"
	tigris_fields "github.com/tigrisdata/tigris-client-go/fields"
	"github.com/tigrisdata/tigris-client-go/filter"
	"github.com/tigrisdata/tigris-client-go/tigris"
)

const (
	tigrisDBName = "tigris.dbname"
	tigrisHost   = "tigris.host"
	tigrisPort   = "tigris.port"
)

type userTable struct {
	Key string `tigris:"primaryKey"`
	//fields []userField
	Field0 []byte
	Field1 []byte
	Field2 []byte
	Field3 []byte
	Field4 []byte
	Field5 []byte
	Field6 []byte
	Field7 []byte
	Field8 []byte
	Field9 []byte
}

//type userField struct {
//	key   string
//	value []byte
//}

type tigrisDB struct {
	db *tigris.Database
}

type tigrisCreator struct {
}

var collection *tigris.Collection[userTable]

func (t *tigrisDB) InitThread(ctx context.Context, threadID int, threadCount int) context.Context {
	return ctx
}

func (t *tigrisDB) CleanupThread(ctx context.Context) {
}

func getReadFields(fields []string) *tigris_fields.Read {
	readFields := tigris_fields.ReadBuilder()
	for _, fieldName := range fields {
		readFields.Include(fieldName)
	}
	return readFields
}

func (t *tigrisDB) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	readFields := getReadFields(fields)

	_, err := collection.ReadOne(ctx, filter.Eq("Key", key), readFields)
	if err != nil {
		return nil, fmt.Errorf("Error while reading key %s.", key)
	}
	return nil, nil
}

func (t *tigrisDB) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	readFields := getReadFields(fields)

	it, err := collection.ReadWithOptions(ctx, filter.Gt("Key", startKey), readFields, &tigris.ReadOptions{Limit: int64(count)})
	if err != nil {
		return nil, fmt.Errorf("Error during scan from %s", startKey)
	}
	defer it.Close()

	return nil, nil
}

func (t *tigrisDB) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	update := tigris_fields.UpdateBuilder()
	for fieldName, fieldValue := range values {
		//TODO: fix the field name capitalization with a non-deprecated function
		update.SetF[strings.Title(fieldName)] = fieldValue
	}
	_, err := collection.Update(ctx, filter.Eq("Key", key), update)
	if err != nil {
		return fmt.Errorf("Error while updating key %s", key)
	}
	return nil
}

func (t *tigrisDB) Insert(ctx context.Context, _ string, key string, values map[string][]byte) error {
	coll := tigris.GetCollection[userTable](t.db)
	_, err := coll.Insert(ctx,
		&userTable{
			Key: key,
			//fields: fields,
			Field0: values["field0"],
			Field1: values["field1"],
			Field2: values["field2"],
			Field3: values["field3"],
			Field4: values["field4"],
			Field5: values["field5"],
			Field6: values["field6"],
			Field7: values["field7"],
			Field8: values["field8"],
			Field9: values["field9"],
		},
	)

	if err != nil {
		return fmt.Errorf("Got error during insert %s!", err.Error())
	}
	return nil
}

func (t *tigrisDB) Delete(ctx context.Context, table string, key string) error {
	_, err := collection.Delete(ctx, filter.Eq("Key", key))
	if err != nil {
		return fmt.Errorf("Error while deleting key %s", key)
	}
	return nil
}

func (c tigrisCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	var conf *config.Database
	ctx := context.Background()
	dbName := p.GetString(tigrisDBName, "ycsb_tigris")
	host := p.GetString(tigrisHost, "localhost")
	port := p.GetInt(tigrisPort, 8081)
	url := fmt.Sprintf("%s:%d", host, port)
	token := os.Getenv("TIGRIS_ACCESS_TOKEN")
	if token != "" {
		conf = &config.Database{Driver: config.Driver{URL: url, Token: token}}
	} else {
		conf = &config.Database{Driver: config.Driver{URL: url}}
	}
	db, err := tigris.OpenDatabase(ctx, conf, dbName, &userTable{})
	if err != nil {
		return nil, fmt.Errorf("Error connecting to tigrisDB: %s", err.Error())
	}
	t := &tigrisDB{
		db: db,
	}
	collection = tigris.GetCollection[userTable](t.db)
	return t, nil
}

func (t *tigrisDB) Close() error {
	return nil
}

func (t *tigrisDB) ToSqlDB() *sql.DB {
	return nil
}

func init() {
	ycsb.RegisterDBCreator("tigris", tigrisCreator{})
}
