package standalone_storage

import (
	"os"
	"path/filepath"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap/errors"
)

// Reference: kv/storage/raft_storage/raft_server.go
// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	// In raft_server, engine_util.Engines is used,
	// which I think is not appropriate to use here.
	db   *badger.DB
	conf *config.Config
}

// Reference: kv/raftstore/raft_storage/region_reader.go
type StandAloneStorageReader struct {
	txn *badger.Txn
}

func (sr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(sr.txn, cf, key)
	// see kv/server/server_test.go:TestRawGetNotFound1
	// If not found, set both err and val nil.
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.txn)
}

func (sr *StandAloneStorageReader) Close() {
	sr.txn.Discard()
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{
		db:   nil,
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	// create DB, simulate with NewRaftStorage
	dbPath := s.conf.DBPath
	storagePath := filepath.Join(dbPath, "StandAlone")
	if err := os.MkdirAll(dbPath, os.ModePerm); err != nil {
		return err
	}
	s.db = engine_util.CreateDB(storagePath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(_ *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	if s.db == nil {
		return nil, errors.Errorf("storage is not initialized yet")
	}
	return &StandAloneStorageReader{
		txn: s.db.NewTransaction(false),
	}, nil
}

func (s *StandAloneStorage) Write(_ *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	if s.db == nil {
		return errors.Errorf("storage is not initialized yet")
	}
	wb := engine_util.WriteBatch{}
	for _, m := range batch {
		wb.SetCF(m.Cf(), m.Key(), m.Value())
	}
	return wb.WriteToDB(s.db)
}
