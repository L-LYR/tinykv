package standalone_storage

import (
	"os"
	"path/filepath"

	"github.com/Connor1996/badger"

	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	//! Your Data Here (1).
	// similar with RaftStorage
	// basic attributes
	engines *engine_util.Engines
	config  *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	//! Your Data Here (1).
	// similar with NewRaftStorage
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	os.MkdirAll(kvPath, os.ModePerm)
	os.MkdirAll(raftPath, os.ModePerm)

	kvDB := engine_util.CreateDB(kvPath, false)
	raftDB := engine_util.CreateDB(raftPath, true)

	engines := engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath)
	return &StandAloneStorage{
		engines: engines,
		config:  conf,
	}
}

func (s *StandAloneStorage) Start() error {
	//! Your Data Here (1).
	//? no need to implement
	return nil
}

func (s *StandAloneStorage) Stop() error {
	//! Your Code Here (1).
	return s.engines.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	//! Your Code Here (1).
	// For read-only transaction, set the param to false.
	return NewBadgerReader(s.engines.Kv.NewTransaction(false)), nil
}

type StandAloneReader struct {
	txn *badger.Txn
}

func NewBadgerReader(txn *badger.Txn) *StandAloneReader {
	return &StandAloneReader{
		txn: txn,
	}
}

//! When the key doesn't exist, return nil for the value
func (reader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(reader.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (reader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.txn)
}

func (reader *StandAloneReader) Close() {
	reader.txn.Discard()
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	//! Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	for _, elem := range batch {
		writeBatch.SetCF(elem.Cf(), elem.Key(), elem.Value())
	}
	return s.engines.WriteKV(writeBatch)
}
