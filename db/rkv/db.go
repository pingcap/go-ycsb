package rkv

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/magiconair/properties"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
)

type rkv struct {
	addr string
}

func (r *rkv) ToSqlDB() *sql.DB {
	return nil
}

func (r *rkv) Close() error {
	return nil
}

func (r *rkv) InitThread(ctx context.Context, _ int, _ int) context.Context {
	return ctx
}

func (r *rkv) CleanupThread(_ context.Context) {
}

func (r *rkv) Read(ctx context.Context, table string, key string, fields []string) (map[string][]byte, error) {
	data := make(map[string][]byte, len(fields))

	resp, err := http.Get(fmt.Sprintf("http://%s/kv?key=%s", rkvAddrDefault, key))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(body, &data)
	if err != nil {
		return nil, err
	}
	return data, err
}

func (r *rkv) Scan(ctx context.Context, table string, startKey string, count int, fields []string) ([]map[string][]byte, error) {
	return nil, fmt.Errorf("scan is not supported")
}

func (r *rkv) Update(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := &http.Client{}
	fmt.Printf("the key is %s and the value is %v\n ", key, values)
	newV := make(map[string][]byte)
	newV["key"] = []byte(key)
	newV["value"] = values["field0"]
	requestBody, err := json.Marshal(newV)
	if err != nil {
		return err
	}

	// Create request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/kv", rkvAddrDefault), bytes.NewBuffer(requestBody))
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Fetch Request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	// Read Response Body
	// respBody, err := ioutil.ReadAll(resp.Body)
	// if err != nil {
	// 	// fmt.Println(err)
	// 	return err
	// }

	// Display Results
	// fmt.Println("response Status : ", resp.Status)
	// fmt.Println("response Headers : ", resp.Header)
	// fmt.Println("response Body : ", string(respBody))
	return nil
}

func (r *rkv) Insert(ctx context.Context, table string, key string, values map[string][]byte) error {
	client := &http.Client{}
	fmt.Printf("the key is %s and the value is %v\n ", key, values)
	newV := make(map[string][]byte)
	newV["key"] = []byte(key)
	newV["value"] = values["field0"]
	requestBody, err := json.Marshal(newV)
	if err != nil {
		return err
	}

	// Create request
	req, err := http.NewRequest("POST", fmt.Sprintf("http://%s/kv", rkvAddrDefault), bytes.NewBuffer(requestBody))
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Fetch Request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	// // Read Response Body
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Display Results
	fmt.Println("response Status : ", resp.Status)
	fmt.Println("response Headers : ", resp.Header)
	fmt.Println("response Body : ", string(respBody))
	return nil
}

func (r *rkv) Delete(ctx context.Context, table string, key string) error {
	client := &http.Client{}

	// Create request
	req, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/kv?key=%s", rkvAddrDefault, key), nil)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Fetch Request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(err)
		return err
	}
	defer resp.Body.Close()

	// Read Response Body
	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// Display Results
	fmt.Println("response Status : ", resp.Status)
	fmt.Println("response Headers : ", resp.Header)
	fmt.Println("response Body : ", string(respBody))
	return nil
}

type rkvCreator struct {
}

func (r rkvCreator) Create(p *properties.Properties) (ycsb.DB, error) {
	rkv := &rkv{}

	return rkv, nil
}

const (
	rkvAddrDefault = "localhost:8090"
)

func init() {
	ycsb.RegisterDBCreator("rkv", rkvCreator{})
}
