package raftstore

import (
	"fmt"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/meta"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

// applyContext
// It is a small part of GlobalContext.
// Kept in applyWorker.
type applyContext struct {
	pr      *router              // used for sending results
	engines *engine_util.Engines // kvdb
}

func newApplyContext(engines *engine_util.Engines, pr *router) *applyContext {
	return &applyContext{
		pr:      pr,
		engines: engines,
	}
}

// applyMsgHandler
// similar with peerMsgHandler
type applyMsgHandler struct {
	*applier
	// ctx *GlobalContext
	aCtx *applyContext
	wb   *engine_util.WriteBatch
	res  message.MsgApplyResult
	cbs  []*message.Callback
}

func newApplyMsgHandler(applier *applier, aCtx *applyContext) *applyMsgHandler {
	curState, err := meta.GetApplyState(aCtx.engines.Kv, applier.region.Id)
	if err != nil {
		log.Panic(err)
	}
	a := &applyMsgHandler{
		applier: applier,
		aCtx:    aCtx,
		wb:      new(engine_util.WriteBatch),
	}
	a.res.ApplyState = curState
	return a
}

func (a *applyMsgHandler) appendResult(r *message.ApplyResult) {
	a.res.Results = append(a.res.Results, *r)
}

func (a *applyMsgHandler) appendCallback(cb *message.Callback) {
	a.cbs = append(a.cbs, cb)
}

func (a *applyMsgHandler) handleApplyMsg(msg message.Msg) {
	// check messageType
	if msg.Type != message.MsgTypeApply {
		log.Panicf("error message type: %s", msg.Type)
	}

	// update applier or deal with stale ApplyCmds
	m := msg.Data.(*message.MsgApply)
	a.term = m.Term

	a.appendCmds(m.Cmds)
	if m.Update {
		m.Entries = nil
		a.update(m)
	}

	if a.removed {
		// log.Infof("%s is removed, clear all", a.tag)
		a.clear()
		return
	}

	a.handle(m)
}

// handle applyCmd
func (a *applyMsgHandler) handle(m *message.MsgApply) {
	log.Debugf("%s begin to apply %d proposals with %d entries", a.tag, len(a.cmds), len(m.Entries))
	for _, e := range m.Entries {
		nextIdx := a.res.ApplyState.AppliedIndex + 1
		if nextIdx != e.Index {
			log.Panicf("want %d index but get %d", nextIdx, e.Index)
		}
		a.apply(&e)
		a.res.ApplyState.AppliedIndex = e.Index
		if a.removed {
			break
		}
	}
	a.done()
}

// After handling all the ApplyCmds, applyMsgHandler will
// write the apply state into DB, trigger all callbacks
// and send apply results to peerMsgHandler.
func (a *applyMsgHandler) done() {
	key := meta.ApplyStateKey(a.region.Id)
	err := a.wb.SetMeta(key, a.res.ApplyState)
	if err != nil {
		log.Panic(err)
	}
	a.wb.MustWriteToDB(a.aCtx.engines.Kv)
	for _, cb := range a.cbs {
		cb.Done(nil)
	}
	// send apply results to peerMsgHandler
	err = a.aCtx.pr.send(a.region.Id, message.Msg{
		Type:     message.MsgTypeApplyResult,
		RegionID: a.region.Id,
		Data:     &a.res,
	})
	if err != nil {
		log.Panic(err)
	}
}

func (a *applyMsgHandler) apply(entry *pb.Entry) {
	// empty entry
	// for example, if there is a pending confChange entry,
	// the following ones will be cleared in raft.appendPropose().
	if entry.Data == nil {
		return
	}

	// confChange has two layers to unmarshal
	req := &raft_cmdpb.RaftCmdRequest{}
	if entry.EntryType == pb.EntryType_EntryConfChange {
		var cc pb.ConfChange
		if err := cc.Unmarshal(entry.Data); err != nil {
			log.Panic(err)
		}
		if err := req.Unmarshal(cc.Context); err != nil {
			log.Panic(err)
		}
	} else {
		if err := req.Unmarshal(entry.Data); err != nil {
			log.Panic(err)
		}
	}

	// check region epoch
	if err := util.CheckRegionEpoch(req, a.applier.region, true); err != nil {
		cb := a.getCallback(entry.Index, entry.Term)
		cb.Done(ErrResp(err))
		return
	}

	if req.AdminRequest != nil {
		a.handleAdminRequests(entry, req.AdminRequest)
	} else {
		a.handleNormalRequests(entry, req)
	}
}

func (a *applyMsgHandler) handleCompactLog(admin *raft_cmdpb.AdminRequest) {
	compactLog := admin.CompactLog
	cIdx, cTerm := compactLog.CompactIndex, compactLog.CompactTerm
	res := a.res
	pIdx := res.ApplyState.TruncatedState.Index // previous truncated index
	if cIdx >= pIdx {
		res.ApplyState.TruncatedState.Index = cIdx
		res.ApplyState.TruncatedState.Term = cTerm
		a.appendResult(&message.ApplyResult{
			ResType:         message.ResType_CompactLog,
			CompactLogIndex: cIdx,
		})
	}
}

func findPeerIdx(peer *metapb.Peer, region *metapb.Region) int {
	for i, p := range region.Peers {
		if p.Id == peer.Id && p.StoreId == peer.StoreId {
			return i
		}
	}
	return -1 // not found
}

// a.region is a pointer, must not be modified directly
func (a *applyMsgHandler) copyRegion() *metapb.Region {
	nRegion := &metapb.Region{} // newRegion
	err := util.CloneMsg(a.region, nRegion)
	if err != nil {
		log.Panic(err)
	}
	return nRegion
}

func (a *applyMsgHandler) handleChangePeer(entry *pb.Entry,
	admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse) {
	var err error
	cp := admin.ChangePeer
	// a.region is a pointer
	nRegion := a.copyRegion()
	idx := findPeerIdx(cp.Peer, nRegion)

	switch cp.ChangeType {
	case pb.ConfChangeType_AddNode:
		if idx == -1 { // not found
			nRegion.Peers = append(nRegion.Peers, cp.Peer)
			nRegion.RegionEpoch.ConfVer++
			log.Infof("%s add peer %d", a.tag, cp.Peer.Id)
		} else { // found
			err = fmt.Errorf("add existing peer %d", cp.Peer.Id)
			BindRespError(resp, err)
			return
		}
	case pb.ConfChangeType_RemoveNode:
		if idx != -1 { // found
			if !util.PeerEqual(cp.Peer, nRegion.Peers[idx]) { // mismatch
				err = fmt.Errorf("mismatched peer expect %s, want %s", cp.Peer, nRegion.Peers[idx])
				BindRespError(resp, err)
				return
			}
			nRegion.Peers = append(nRegion.Peers[:idx], nRegion.Peers[idx+1:]...)
			nRegion.RegionEpoch.ConfVer++
			log.Infof("%s remove peer %s", a.tag, cp.Peer)
			if a.id == cp.Peer.Id {
				a.removed = true
			}
		} else { // not found
			err = fmt.Errorf("remove non-existing peer %s", cp.Peer)
			BindRespError(resp, err)
			return
		}
	}

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_ChangePeer,
		ChangePeer: &raft_cmdpb.ChangePeerResponse{
			Region: nRegion,
		},
	}

	cc := &pb.ConfChange{}
	_ = cc.Unmarshal(entry.Data)
	a.appendResult(&message.ApplyResult{
		ResType:    message.ResType_ChangePeer,
		Peer:       cp.Peer,
		NewRegion:  nRegion,
		ConfChange: cc,
	})

	// update itself
	a.region = nRegion
	if a.removed {
		meta.WriteRegionState(a.wb, nRegion, rspb.PeerState_Tombstone)
	} else {
		meta.WriteRegionState(a.wb, nRegion, rspb.PeerState_Normal)
	}
}

func (a *applyMsgHandler) handleSplitRegion(admin *raft_cmdpb.AdminRequest, resp *raft_cmdpb.RaftCmdResponse) {
	var err error
	oldRegion := a.copyRegion()
	split := admin.Split
	// check key length ?
	//if len(key) == 0 {
	//	err = errors.New("split key is missing")
	//	BindRespError(resp, err)
	//	return
	//}
	key := split.SplitKey
	// check whether the key is in the previous region
	if err = util.CheckKeyInRegion(key, oldRegion); err != nil {
		BindRespError(resp, err)
		return
	}
	// check peer ids, must has the same number
	if len(split.NewPeerIds) != len(oldRegion.Peers) {
		err = fmt.Errorf("mismatch peers, expect %d peers, get %d peers",
			len(oldRegion.Peers), len(split.NewPeerIds))
		BindRespError(resp, err)
		return
	}

	// update region epoch
	oldRegion.RegionEpoch.Version++

	// copy new peers
	newPeers := make([]*metapb.Peer, 0)
	for i, p := range oldRegion.Peers {
		newPeers = append(newPeers, &metapb.Peer{
			Id:      split.NewPeerIds[i],
			StoreId: p.StoreId,
		})
	}

	// generate new region
	newRegion := &metapb.Region{
		Id:       split.NewRegionId,
		StartKey: util.SafeCopy(split.SplitKey),
		EndKey:   util.SafeCopy(oldRegion.EndKey),
		RegionEpoch: &metapb.RegionEpoch{
			ConfVer: oldRegion.RegionEpoch.ConfVer,
			Version: oldRegion.RegionEpoch.Version,
		},
		Peers: newPeers,
	}

	// split keys
	//    |-------previous------|
	//    |---old----|----new---|
	// start--------split------end
	oldRegion.EndKey = util.SafeCopy(split.SplitKey)

	// write region state
	// remember to write both of old and new
	meta.WriteRegionState(a.wb, newRegion, rspb.PeerState_Normal)
	meta.WriteRegionState(a.wb, oldRegion, rspb.PeerState_Normal)

	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType: raft_cmdpb.AdminCmdType_Split,
		Split: &raft_cmdpb.SplitResponse{
			Regions: []*metapb.Region{oldRegion, newRegion},
		},
	}

	a.appendResult(&message.ApplyResult{
		ResType:   message.ResType_SplitRegion,
		NewRegion: newRegion,
		OldRegion: oldRegion,
	})

	a.region = oldRegion
	// An inaccurate difference in region size since last reset.
	// Reset SizeDiffHint here.
	a.res.SizeDiffHint = 0
}

func (a *applyMsgHandler) handleAdminRequests(entry *pb.Entry, admin *raft_cmdpb.AdminRequest) {
	cb := a.getCallback(entry.Index, entry.Term)
	resp := newCmdResp()
	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		a.handleCompactLog(admin) // no response for cb == nil
	case raft_cmdpb.AdminCmdType_ChangePeer:
		a.handleChangePeer(entry, admin, resp) // entry for pb.ConfChange
	case raft_cmdpb.AdminCmdType_Split:
		a.handleSplitRegion(admin, resp)
	}
	if cb != nil {
		cb.Resp = resp
		a.appendCallback(cb)
	}
}

func (a *applyMsgHandler) handleNormalRequests(entry *pb.Entry, req *raft_cmdpb.RaftCmdRequest) {
	cb := a.applier.getCallback(entry.Index, entry.Term)
	hasRead, hasWrite := false, false

	for _, r := range req.Requests {
		var key []byte
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = r.Get.Key
			hasRead = true
		case raft_cmdpb.CmdType_Put:
			key = r.Put.Key
			hasWrite = true
		case raft_cmdpb.CmdType_Delete:
			key = r.Delete.Key
			hasWrite = true
		case raft_cmdpb.CmdType_Snap:
			key = nil
			hasRead = true
		}
		if key == nil {
			continue
		}
		// Confusing: Is it useless to check whether the key is in region here?
		// It is checked in peerMsgHandler.proposeRaftCommand
		if err := util.CheckKeyInRegion(key, a.region); err != nil {
			log.Info("request's key is not in region")
			cb.Done(ErrResp(err))
			return
		}
	}

	if hasWrite && hasRead {
		log.Panic("write and read requests mixed in one entry")
	}

	resp := newCmdResp()
	for _, r := range req.Requests {
		res := &raft_cmdpb.Response{
			CmdType: r.CmdType,
		}
		var txn *badger.Txn = nil
		var diffSize int = 0
		switch r.CmdType {
		case raft_cmdpb.CmdType_Put:
			putReq := r.Put
			a.wb.SetCF(putReq.Cf, putReq.Key, putReq.Value)
			diffSize = len(putReq.Cf) + len(putReq.Key) + len(putReq.Value)
			a.res.SizeDiffHint += int64(diffSize)
			//log.Infof("%s put %s %s %s", a.tag, putReq.Cf, putReq.Key, putReq.Value)
		case raft_cmdpb.CmdType_Delete:
			deleteReq := r.Delete
			a.wb.DeleteCF(deleteReq.Cf, deleteReq.Key)
			diffSize = len(deleteReq.Cf) + len(deleteReq.Key)
			a.res.SizeDiffHint -= int64(diffSize)
			//log.Infof("%s delete %s %s", a.tag, deleteReq.Cf, deleteReq.Key)
		case raft_cmdpb.CmdType_Get:
			getReq := r.Get
			val, _ := engine_util.GetCF(a.aCtx.engines.Kv, getReq.Cf, getReq.Key)
			res.Get = &raft_cmdpb.GetResponse{Value: val}
			//log.Infof("%s get %s %s %s", a.tag, getReq.Cf, getReq.Key, val)
		case raft_cmdpb.CmdType_Snap:
			txn = a.aCtx.engines.Kv.NewTransaction(false)
			res.Snap = &raft_cmdpb.SnapResponse{Region: a.region}
		}
		if txn != nil && cb != nil {
			cb.Txn = txn
		}
		resp.Responses = append(resp.Responses, res)
	}

	if cb != nil {
		cb.Resp = resp
		a.appendCallback(cb)
	}
}

//func (a *applyMsgHandler) response(r *raft_cmdpb.Request) (*raft_cmdpb.Response, *badger.Txn) {
//	res := &raft_cmdpb.Response{
//		CmdType: r.CmdType,
//	}
//	var txn *badger.Txn = nil
//	switch r.CmdType {
//	case raft_cmdpb.CmdType_Get:
//		a.doCurrentWB()
//		getReq := r.Get
//		val, _ := engine_util.GetCF(a.aCtx.engines.Kv, getReq.Cf, getReq.Key)
//		res.Get = &raft_cmdpb.GetResponse{Value: val}
//		log.Infof("%s get %s %s %s", a.tag, getReq.Cf, getReq.Key, val)
//	case raft_cmdpb.CmdType_Snap:
//		a.doCurrentWB()
//		txn = a.aCtx.engines.Kv.NewTransaction(false)
//		res.Snap = &raft_cmdpb.SnapResponse{Region: a.region}
//	case raft_cmdpb.CmdType_Put:
//		res.Put = &raft_cmdpb.PutResponse{}
//	case raft_cmdpb.CmdType_Delete:
//		res.Delete = &raft_cmdpb.DeleteResponse{}
//	}
//	return res, txn
//}

//func (a *applyMsgHandler) doCurrentWB() {
//	//log.Infof("%d write to db", a.id)
//	key := meta.ApplyStateKey(a.region.Id)
//	err := a.aCtx.wb.SetMeta(key, a.aCtx.res.applyState)
//	if err != nil {
//		log.Panic(err)
//	}
//	a.aCtx.wb.MustWriteToDB(a.aCtx.engines.Kv)
//	a.aCtx.wb.Reset()
//}
