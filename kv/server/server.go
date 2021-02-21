package server

import (
	"context"
	"fmt"
	"reflect"

	"github.com/pingcap-incubator/tinykv/kv/transaction/mvcc"

	"github.com/pingcap-incubator/tinykv/kv/coprocessor"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/storage/raft_storage"
	"github.com/pingcap-incubator/tinykv/kv/transaction/latches"
	coppb "github.com/pingcap-incubator/tinykv/proto/pkg/coprocessor"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/tinykvpb"
	"github.com/pingcap/tidb/kv"
)

var _ tinykvpb.TinyKvServer = new(Server)

// Server is a TinyKV server, it 'faces outwards', sending and receiving messages from clients such as TinySQL.
type Server struct {
	storage storage.Storage

	// (Used in 4A/4B)
	Latches *latches.Latches

	// coprocessor API handler, out of course scope
	copHandler *coprocessor.CopHandler
}

func NewServer(storage storage.Storage) *Server {
	return &Server{
		storage: storage,
		Latches: latches.NewLatches(),
	}
}

// The below functions are Server's gRPC API (implements TinyKvServer).

// It is mentioned in project4 doc that any request might cause a region error.
// But there is not a interface for all kinds of response types, so we use
// reflect here to bind the region error to resp.
func bindError(resp interface{}, err error) {
	res := reflect.ValueOf(resp)
	if castRes, ok := err.(*raft_storage.RegionError); ok {
		res.FieldByName("RegionError").Set(reflect.ValueOf(castRes))
	} else {
		res.FieldByName("Error").SetString(err.Error())
	}
}

// Raw API.
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	// Your Code Here (1).
	ss := server.storage
	resp := &kvrpcpb.RawGetResponse{}

	sr, err := ss.Reader(req.GetContext())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}
	defer sr.Close()

	val, err := sr.GetCF(req.GetCf(), req.GetKey())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}
	resp.Value = val
	// If not found, both err and val will be nil
	resp.NotFound = val == nil

	return resp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	ss := server.storage
	resp := &kvrpcpb.RawPutResponse{}

	err := ss.Write(req.GetContext(), []storage.Modify{{
		Data: storage.Put{
			Cf:    req.GetCf(),
			Key:   req.GetKey(),
			Value: req.GetValue(),
		},
	}})

	if err != nil {
		bindError(resp, err)
		return resp, err
	}

	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	// Your Code Here (1).
	ss := server.storage
	resp := &kvrpcpb.RawDeleteResponse{}

	err := ss.Write(req.GetContext(), []storage.Modify{{
		Data: storage.Delete{
			Key: req.GetKey(),
			Cf:  req.GetCf(),
		},
	}})

	if err != nil {
		bindError(resp, err)
		return resp, err
	}

	return resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	ss := server.storage
	resp := &kvrpcpb.RawScanResponse{}

	sr, err := ss.Reader(req.GetContext())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}
	defer sr.Close()

	dbIter := sr.IterCF(req.GetCf())
	defer dbIter.Close()

	dbIter.Seek(req.GetStartKey())
	for i := uint32(0); i < req.GetLimit() && dbIter.Valid(); i++ {
		kvPair := kvrpcpb.KvPair{}

		curItem := dbIter.Item()
		key := curItem.KeyCopy(nil)
		val, err := curItem.ValueCopy(nil)
		if err != nil {
			bindError(resp, err)
			return resp, err
		}
		kvPair.Key = key
		kvPair.Value = val

		resp.Kvs = append(resp.Kvs, &kvPair)
		dbIter.Next()
	}
	return resp, nil
}

// Raft commands (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Raft(stream tinykvpb.TinyKv_RaftServer) error {
	return server.storage.(*raft_storage.RaftStorage).Raft(stream)
}

// Snapshot stream (tinykv <-> tinykv)
// Only used for RaftStorage, so trivially forward it.
func (server *Server) Snapshot(stream tinykvpb.TinyKv_SnapshotServer) error {
	return server.storage.(*raft_storage.RaftStorage).Snapshot(stream)
}

// Transactional API.
func (server *Server) KvGet(_ context.Context, req *kvrpcpb.GetRequest) (*kvrpcpb.GetResponse, error) {
	// Your Code Here (4B).
	ss := server.storage
	resp := &kvrpcpb.GetResponse{}
	sr, err := ss.Reader(req.GetContext())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}
	defer sr.Close()

	txn := mvcc.NewMvccTxn(sr, req.GetVersion())
	lock, err := txn.GetLock(req.GetKey())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}

	// check whether the target key is locked
	if lock != nil && lock.IsLockedFor(req.GetKey(), txn.StartTS, resp) {
		return resp, nil
	}

	val, err := txn.GetValue(req.GetKey())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}

	resp.Value = val
	resp.NotFound = val == nil
	return resp, nil
}

func (server *Server) KvPrewrite(_ context.Context, req *kvrpcpb.PrewriteRequest) (*kvrpcpb.PrewriteResponse, error) {
	// Your Code Here (4B).
	ss := server.storage
	resp := &kvrpcpb.PrewriteResponse{}
	sr, err := ss.Reader(req.GetContext())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}
	defer sr.Close()

	txn := mvcc.NewMvccTxn(sr, req.GetStartVersion())

	// get keys
	var keys [][]byte
	for _, k := range req.GetMutations() {
		keys = append(keys, k.GetKey())
	}

	// waite for latches
	ls := server.Latches
	ls.WaitForLatches(keys)
	defer ls.ReleaseLatches(keys)

	var keyError *kvrpcpb.KeyError = nil
	for _, m := range req.GetMutations() {
		keyError, err = txn.CheckKeyConflict(m.GetKey(), req.GetPrimaryLock())
		if err != nil { // occur an error
			bindError(resp, err)
			return resp, err
		}

		// not conflict, check for lock
		if keyError == nil {
			keyError, _, err = txn.CheckKeyLocked(m.GetKey())
		}

		if err != nil { // occur an error
			bindError(resp, err)
			return resp, err
		}

		if keyError != nil { // conflict or locked
			resp.Errors = append(resp.Errors, keyError)
			continue
		}

		// no key error
		txn.PutLock(m.GetKey(), &mvcc.Lock{
			Primary: req.GetPrimaryLock(),
			Ts:      txn.StartTS,
			Ttl:     req.GetLockTtl(),
			Kind:    mvcc.WriteKindFromProto(m.GetOp()),
		})
		txn.PutValue(m.GetKey(), m.GetValue())
	}

	// only for test
	ls.Validation(txn, keys)

	// write
	if err = ss.Write(req.GetContext(), txn.Writes()); err != nil {
		bindError(resp, err)
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvCommit(_ context.Context, req *kvrpcpb.CommitRequest) (*kvrpcpb.CommitResponse, error) {
	// Your Code Here (4B).
	ss := server.storage
	resp := &kvrpcpb.CommitResponse{}
	sr, err := ss.Reader(req.GetContext())
	if err != nil {
		bindError(resp, err)
		return resp, err
	}
	defer sr.Close()

	txn := mvcc.NewMvccTxn(sr, req.GetStartVersion())

	ls := server.Latches
	ls.WaitForLatches(req.GetKeys())
	defer ls.ReleaseLatches(req.GetKeys())
	// same with prewrite

	for _, key := range req.GetKeys() {
		// check for lock
		keyError, lock, err := txn.CheckKeyLocked(key)
		if err != nil { // occur an error
			bindError(resp, err)
			return resp, err
		}
		if lock == nil { // not locked, fail to commit
			return resp, nil
		}
		if keyError != nil { // locked by another txn
			// It is mentioned in kvrpcpb.proto that
			// client may restart the txn. e.g write conflict.
			// So here we should set the keyError.retryable.
			keyError.Retryable = fmt.Sprintf("Locked by another txn.")
			resp.Error = keyError
			return resp, nil
		}
		// commit
		txn.PutWrite(key, req.GetCommitVersion(), &mvcc.Write{
			StartTS: txn.StartTS,
			Kind:    lock.Kind,
		})
		txn.DeleteLock(key)
	}

	ls.Validation(txn, req.GetKeys())

	// write
	if err = ss.Write(req.GetContext(), txn.Writes()); err != nil {
		bindError(resp, err)
		return resp, err
	}

	return resp, nil
}

func (server *Server) KvScan(_ context.Context, req *kvrpcpb.ScanRequest) (*kvrpcpb.ScanResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvCheckTxnStatus(_ context.Context, req *kvrpcpb.CheckTxnStatusRequest) (*kvrpcpb.CheckTxnStatusResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvBatchRollback(_ context.Context, req *kvrpcpb.BatchRollbackRequest) (*kvrpcpb.BatchRollbackResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

func (server *Server) KvResolveLock(_ context.Context, req *kvrpcpb.ResolveLockRequest) (*kvrpcpb.ResolveLockResponse, error) {
	// Your Code Here (4C).
	return nil, nil
}

// SQL push down commands.
func (server *Server) Coprocessor(_ context.Context, req *coppb.Request) (*coppb.Response, error) {
	resp := new(coppb.Response)
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		if regionErr, ok := err.(*raft_storage.RegionError); ok {
			resp.RegionError = regionErr.RequestErr
			return resp, nil
		}
		return nil, err
	}
	switch req.Tp {
	case kv.ReqTypeDAG:
		return server.copHandler.HandleCopDAGRequest(reader, req), nil
	case kv.ReqTypeAnalyze:
		return server.copHandler.HandleCopAnalyzeRequest(reader, req), nil
	}
	return nil, nil
}
