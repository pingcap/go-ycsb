package taas_leveldb

//#include ""
import (
	"context"
)

var TaasServerIp = "127.0.0.1"
var LocalServerIp = "127.0.0.1"
var HbaseServerIp = "127.0.0.1"
var OpNum = 10

type TaasTxn struct {
	GzipedTransaction []byte
}

var TaasTxnCH = make(chan TaasTxn, 100000)
var UnPackCH = make(chan string, 100000)

var ChanList []chan string
var InitOk uint64 = 0
var atomicCounter uint64 = 0
var SuccessTransactionCounter, FailedTransactionCounter, TotalTransactionCounter uint64 = 0, 0, 0
var SuccessReadCounter, FailedReadCounter, TotalReadCounter uint64 = 0, 0, 0
var SuccessUpdateCounter, FailedUpdateounter, TotalUpdateCounter uint64 = 0, 0, 0
var TotalLatency, TikvReadLatency, TikvTotalLatency uint64 = 0, 0, 0
var latency []uint64

func (db *txnDB) CommitToTaas(ctx context.Context, table string, keys []string, values []map[string][]byte) error {
	return nil
}

func SendTxnToTaas() {
}

func ListenFromTaas() {
}

func UGZipBytes(in []byte) []byte {
	return nil
}

func UnPack() {
}
