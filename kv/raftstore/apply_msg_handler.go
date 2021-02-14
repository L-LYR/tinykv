package raftstore

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

// apply msgs
type MsgApplyProposal struct {
	proposals []*proposal
	entries   []pb.Entry
}

// applyContext
type applyContext struct {
	engines *engine_util.Engines
	wb      *engine_util.WriteBatch
	//res
	cbs []*message.Callback
}

func newApplyContext(engines *engine_util.Engines) *applyContext {
	return &applyContext{
		engines: engines,
		wb:      new(engine_util.WriteBatch),
	}
}

func (ac *applyContext) appendCallback(cb *message.Callback) {
	ac.cbs = append(ac.cbs, cb)
}

func (ac *applyContext) done() {
	for _, cb := range ac.cbs {
		cb.Done(nil)
	}
	ac.cbs = ac.cbs[:0]
}

// applyMsgHandler
// similar with peerMsgHandler
type applyMsgHandler struct {
	*applier
	applyState *rspb.RaftApplyState
	// ctx *GlobalContext
	aCtx *applyContext
}

func newApplyMsgHandler(applier *applier, aCtx *applyContext) *applyMsgHandler {
	curState, err := meta.GetApplyState(aCtx.engines.Kv, applier.region.Id)
	if err != nil {
		log.Panic(err)
	}
	return &applyMsgHandler{
		applier:    applier,
		aCtx:       aCtx,
		applyState: curState,
	}
}

func (a *applyMsgHandler) handleApplyMsg(msg message.Msg) {
	if msg.Type == message.MsgTypeApplyProposals {
		m := msg.Data.(*MsgApplyProposal)
		a.appendProposals(m.proposals)
		a.handleProposal(m)
	} else {
		log.Panicf("error message type: %s", msg.Type)
	}
}

func (a *applyMsgHandler) handleProposal(m *MsgApplyProposal) {
	log.Debugf("%s begin to apply %d proposals with %d entries", a.tag, len(a.proposals), len(m.entries))
	for _, e := range m.entries {
		nextIdx := a.applyState.AppliedIndex + 1
		if nextIdx != e.Index {
			log.Panicf("want %d index but get %d", nextIdx, e.Index)
		}
		a.apply(&e)
		a.applyState.AppliedIndex = e.Index
	}
	a.doCurrentWB()
	a.aCtx.done()
}

func (a *applyMsgHandler) apply(entry *pb.Entry) {
	if entry.Data == nil {
		return
	}
	req := &raft_cmdpb.RaftCmdRequest{}
	if err := req.Unmarshal(entry.Data); err != nil {
		log.Panic(err)
	}
	if req.AdminRequest != nil {
		// admin request
	} else if len(req.Requests) > 0 {
		a.handleNormalRequests(entry, req)
	}
}

func (a *applyMsgHandler) handleNormalRequests(entry *pb.Entry, req *raft_cmdpb.RaftCmdRequest) {
	cb := a.applier.getCallback(entry.Index, entry.Term)
	resp := newCmdResp()
	for _, r := range req.Requests {
		switch r.CmdType {
		case raft_cmdpb.CmdType_Put:
			putReq := r.Put
			a.aCtx.wb.SetCF(putReq.Cf, putReq.Key, putReq.Value)
		case raft_cmdpb.CmdType_Delete:
			deleteReq := r.Delete
			a.aCtx.wb.DeleteCF(deleteReq.Cf, deleteReq.Key)
		}
		res, txn := a.response(r)
		if txn != nil && cb != nil {
			cb.Txn = txn
		}
		resp.Responses = append(resp.Responses, res)
	}

	if cb != nil {
		cb.Resp = resp
		a.aCtx.appendCallback(cb)
	}
}

func (a *applyMsgHandler) response(r *raft_cmdpb.Request) (*raft_cmdpb.Response, *badger.Txn) {
	res := &raft_cmdpb.Response{
		CmdType: r.CmdType,
	}
	var txn *badger.Txn = nil
	switch r.CmdType {
	case raft_cmdpb.CmdType_Get:
		a.doCurrentWB()
		getReq := r.Get
		val, _ := engine_util.GetCF(a.aCtx.engines.Kv, getReq.Cf, getReq.Key)
		res.Get = &raft_cmdpb.GetResponse{Value: val}
	case raft_cmdpb.CmdType_Snap:
		a.doCurrentWB()
		txn = a.aCtx.engines.Kv.NewTransaction(false)
		res.Snap = &raft_cmdpb.SnapResponse{Region: a.region}
	case raft_cmdpb.CmdType_Put:
		res.Put = &raft_cmdpb.PutResponse{}
	case raft_cmdpb.CmdType_Delete:
		res.Delete = &raft_cmdpb.DeleteResponse{}
	}
	return res, txn
}

func (a *applyMsgHandler) doCurrentWB() {
	key := meta.ApplyStateKey(a.region.Id)
	err := a.aCtx.wb.SetMeta(key, a.applyState)
	if err != nil {
		log.Panic(err)
	}
	a.aCtx.wb.MustWriteToDB(a.aCtx.engines.Kv)
	a.aCtx.wb.Reset()
}
