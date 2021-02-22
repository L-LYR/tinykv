package raftstore

import (
	"fmt"
	"time"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"

	"github.com/Connor1996/badger/y"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/runner"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/snap"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/raft_cmdpb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
	"github.com/pingcap-incubator/tinykv/scheduler/pkg/btree"
	"github.com/pingcap/errors"
)

type PeerTick int

const (
	PeerTickRaft               PeerTick = 0
	PeerTickRaftLogGC          PeerTick = 1
	PeerTickSplitRegionCheck   PeerTick = 2
	PeerTickSchedulerHeartbeat PeerTick = 3
)

type peerMsgHandler struct {
	*peer
	ctx *GlobalContext
	// this is used to apply msgs asynchronously.
	applyCh chan []message.Msg
}

func newPeerMsgHandler(peer *peer, applyCh chan []message.Msg, ctx *GlobalContext) *peerMsgHandler {
	return &peerMsgHandler{
		peer:    peer,
		ctx:     ctx,
		applyCh: applyCh,
	}
}

func (d *peerMsgHandler) HandleRaftReady() {
	// Your Code Here (2B).
	// for {
	//  select {
	//  case <-s.Ticker:
	//    Node.Tick()
	//  default:
	//    if Node.HasReady() {
	//      rd := Node.Ready()
	//      saveToStorage(rd.State, rd.Entries, rd.Snapshot)
	//      send(rd.Messages)
	//      for _, entry := range rd.CommittedEntries {
	//        process(entry)
	//      }
	//      s.Node.Advance(rd)
	//    }
	// }
	if d.stopped {
		return
	}

	// still applying pending snap
	if d.RaftGroup.HasSnap() &&
		d.LastApplyingIndex != d.peerStorage.applyState.AppliedIndex {
		log.Infof("%s has pending snapshot", d.Tag)
		return
	}

	if !d.RaftGroup.HasReady() {
		return
	}

	applyMsgs := make([]message.Msg, 0)
	rd := d.RaftGroup.Ready()

	res, err := d.peerStorage.SaveReadyState(&rd)
	if err != nil {
		log.Panic(err)
	}
	if res != nil {
		// previous region & current region
		pRegion, cRegion := res.PrevRegion, res.Region
		log.Debugf("%s snapshot for region %s is applied", d.Tag, cRegion)
		m := d.ctx.storeMeta
		m.Lock()
		m.setRegion(cRegion, d.peer)
		if len(pRegion.Peers) > 0 {
			log.Debugf("region changed from %s to %s", pRegion, cRegion)
			m.regionRanges.Delete(&regionItem{region: pRegion})
		}
		m.regionRanges.ReplaceOrInsert(&regionItem{region: cRegion})
		m.Unlock()
		d.LastApplyingIndex = d.peerStorage.AppliedIndex()
	}

	d.Send(d.ctx.trans, rd.Messages)

	// wrap proposal in ApplyCmd
	// TODO: proposal is just the same with ApplyCmd; using one of them is enough.
	cmds := make([]*message.ApplyCmd, 0)
	for _, p := range d.proposals {
		cmds = append(cmds, &message.ApplyCmd{
			Index: p.index,
			Term:  p.term,
			Cb:    p.cb,
		})
	}

	applyMsgs = append(applyMsgs, message.Msg{
		Type:     message.MsgTypeApply,
		RegionID: d.regionId,
		Data: &message.MsgApply{
			Id:      d.PeerId(),
			Term:    d.Term(),
			Region:  d.Region(),
			Update:  res != nil, // no snapshot
			Cmds:    cmds,
			Entries: rd.CommittedEntries,
		},
	})
	if curLen := len(rd.CommittedEntries); curLen > 0 && res == nil {
		d.LastApplyingIndex = rd.CommittedEntries[curLen-1].Index
	}
	d.proposals = nil

	d.applyCh <- applyMsgs
	d.RaftGroup.Advance(rd)
}

func (d *peerMsgHandler) HandleMsg(msg message.Msg) {
	switch msg.Type {
	case message.MsgTypeRaftMessage:
		raftMsg := msg.Data.(*rspb.RaftMessage)
		if err := d.onRaftMsg(raftMsg); err != nil {
			log.Errorf("%s handle raft message error %v", d.Tag, err)
		}
	case message.MsgTypeRaftCmd:
		raftCMD := msg.Data.(*message.MsgRaftCmd)
		d.proposeRaftCommand(raftCMD.Request, raftCMD.Callback)
	case message.MsgTypeTick:
		d.onTick()
	case message.MsgTypeSplitRegion:
		split := msg.Data.(*message.MsgSplitRegion)
		log.Infof("%s on split with %v", d.Tag, split.SplitKey)
		d.onPrepareSplitRegion(split.RegionEpoch, split.SplitKey, split.Callback)
	case message.MsgTypeRegionApproximateSize:
		d.onApproximateRegionSize(msg.Data.(uint64))
	case message.MsgTypeGcSnap:
		gcSnap := msg.Data.(*message.MsgGCSnap)
		d.onGCSnap(gcSnap.Snaps)
	case message.MsgTypeStart:
		d.startTicker()
	case message.MsgTypeApplyResult:
		applyRes := msg.Data.(*message.MsgApplyResult)
		d.onApplyResult(applyRes)
	}
}

func (d *peerMsgHandler) preProposeRaftCommand(req *raft_cmdpb.RaftCmdRequest) error {
	// Check store_id, make sure that the msg is dispatched to the right place.
	if err := util.CheckStoreID(req, d.storeID()); err != nil {
		return err
	}

	// Check whether the store has the right peer to handle the request.
	regionID := d.regionId
	leaderID := d.LeaderId()
	if !d.IsLeader() {
		leader := d.getPeerFromCache(leaderID)
		return &util.ErrNotLeader{RegionId: regionID, Leader: leader}
	}
	// peer_id must be the same as peer's.
	if err := util.CheckPeerID(req, d.PeerId()); err != nil {
		return err
	}
	// Check whether the term is stale.
	if err := util.CheckTerm(req, d.Term()); err != nil {
		return err
	}
	err := util.CheckRegionEpoch(req, d.Region(), true)
	if errEpochNotMatching, ok := err.(*util.ErrEpochNotMatch); ok {
		// Attach the region which might be split from the current region. But it doesn't
		// matter if the region is not split from the current region. If the region meta
		// received by the TiKV driver is newer than the meta cached in the driver, the meta is
		// updated.
		siblingRegion := d.findSiblingRegion()
		if siblingRegion != nil {
			errEpochNotMatching.Regions = append(errEpochNotMatching.Regions, siblingRegion)
		}
		return errEpochNotMatching
	}
	return err
}

func (d *peerMsgHandler) appendProposal(idx uint64, cb *message.Callback) {
	d.proposals = append(d.proposals, &proposal{
		index: idx,
		term:  d.Term(),
		cb:    cb,
	})
}

func (d *peerMsgHandler) proposeReq(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	var err error
	data, err := req.Marshal()
	if err != nil {
		log.Panic(err)
	}
	idx := d.nextProposalIndex()
	err = d.RaftGroup.Propose(data)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.appendProposal(idx, cb)
}

func (d *peerMsgHandler) handleLeaderTransfer(id uint64, cb *message.Callback) {
	d.RaftGroup.TransferLeader(id)
	resp := newCmdResp()
	resp.AdminResponse = &raft_cmdpb.AdminResponse{
		CmdType:        raft_cmdpb.AdminCmdType_TransferLeader,
		TransferLeader: &raft_cmdpb.TransferLeaderResponse{},
	}
	cb.Done(resp)
}

// TODO: use proposeReq in proposeChangePeer, make all the propose function more simple and uniform
func (d *peerMsgHandler) proposeChangePeer(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	var err error
	// has pending conf change msg
	if d.RaftGroup.Raft.PendingConfIndex >
		d.peerStorage.applyState.AppliedIndex {
		err = errors.New("Has pending config change msg")
		cb.Done(ErrResp(err))
		return
	}
	data, err := req.Marshal()
	if err != nil {
		log.Panic(err)
	}
	idx := d.nextProposalIndex()
	cp := req.AdminRequest.ChangePeer
	err = d.RaftGroup.ProposeConfChange(pb.ConfChange{
		ChangeType: cp.ChangeType,
		NodeId:     cp.Peer.Id,
		Context:    data,
	})
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.appendProposal(idx, cb)
}

func (d *peerMsgHandler) proposeSplit(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	key := req.AdminRequest.Split.SplitKey
	if err := util.CheckKeyInRegion(key, d.Region()); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	d.proposeReq(req, cb)
}

func (d *peerMsgHandler) proposeAdminReq(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	admin := req.AdminRequest
	switch admin.CmdType {
	case raft_cmdpb.AdminCmdType_CompactLog:
		// CompactLog is just like the normal request
		// Actually it has no callback.
		d.proposeReq(req, cb)
	case raft_cmdpb.AdminCmdType_TransferLeader:
		// handled in the previous layer
		d.handleLeaderTransfer(admin.TransferLeader.Peer.Id, cb)
	case raft_cmdpb.AdminCmdType_ChangePeer:
		d.proposeChangePeer(req, cb)
	case raft_cmdpb.AdminCmdType_Split:
		d.proposeSplit(req, cb)
	default:
		log.Panicf("Unreachable")
	}
}

func (d *peerMsgHandler) proposeNormalReq(req *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	region := d.Region()
	for _, r := range req.Requests {
		var key []byte
		switch r.CmdType {
		case raft_cmdpb.CmdType_Get:
			key = r.Get.Key
		case raft_cmdpb.CmdType_Put:
			key = r.Put.Key
		case raft_cmdpb.CmdType_Delete:
			key = r.Delete.Key
		case raft_cmdpb.CmdType_Snap:
			key = nil
		}
		if key == nil {
			continue
		}
		// must check key here
		// Or assertion will be triggered in OneSplitTest
		if err := util.CheckKeyInRegion(key, region); err != nil {
			log.Info("request's key is not in region")
			cb.Done(ErrResp(err))
			return
		}
	}
	d.proposeReq(req, cb)
}

func (d *peerMsgHandler) proposeRaftCommand(msg *raft_cmdpb.RaftCmdRequest, cb *message.Callback) {
	err := d.preProposeRaftCommand(msg)
	if err != nil {
		cb.Done(ErrResp(err))
		return
	}
	// check region
	if d.stopped {
		NotifyReqRegionRemoved(d.regionId, cb)
		return
	}
	// Your Code Here (2B).
	// bind term
	// identify what kind of request it is.
	if msg.AdminRequest != nil {
		d.proposeAdminReq(msg, cb)
	} else if len(msg.Requests) > 0 {
		d.proposeNormalReq(msg, cb)
	}
}

func (d *peerMsgHandler) onTick() {
	if d.stopped {
		return
	}
	d.ticker.tickClock()
	if d.ticker.isOnTick(PeerTickRaft) {
		d.onRaftBaseTick()
	}
	if d.ticker.isOnTick(PeerTickRaftLogGC) {
		d.onRaftGCLogTick()
	}
	if d.ticker.isOnTick(PeerTickSchedulerHeartbeat) {
		d.onSchedulerHeartbeatTick()
	}
	if d.ticker.isOnTick(PeerTickSplitRegionCheck) {
		d.onSplitRegionCheckTick()
	}
	d.ctx.tickDriverSender <- d.regionId
}

func (d *peerMsgHandler) startTicker() {
	d.ticker = newTicker(d.regionId, d.ctx.cfg)
	d.ctx.tickDriverSender <- d.regionId
	d.ticker.schedule(PeerTickRaft)
	d.ticker.schedule(PeerTickRaftLogGC)
	d.ticker.schedule(PeerTickSplitRegionCheck)
	d.ticker.schedule(PeerTickSchedulerHeartbeat)
}

func (d *peerMsgHandler) onRaftBaseTick() {
	d.RaftGroup.Tick()
	d.ticker.schedule(PeerTickRaft)
}

func (d *peerMsgHandler) ScheduleCompactLog(truncatedIndex uint64) {
	raftLogGCTask := &runner.RaftLogGCTask{
		RaftEngine: d.ctx.engine.Raft,
		RegionID:   d.regionId,
		StartIdx:   d.LastCompactedIdx,
		EndIdx:     truncatedIndex + 1,
	}
	d.LastCompactedIdx = raftLogGCTask.EndIdx
	d.ctx.raftLogGCTaskSender <- raftLogGCTask
}

func (d *peerMsgHandler) onRaftMsg(msg *rspb.RaftMessage) error {
	log.Debugf("%s handle raft message %s from %d to %d",
		d.Tag, msg.GetMessage().GetMsgType(), msg.GetFromPeer().GetId(), msg.GetToPeer().GetId())
	if !d.validateRaftMessage(msg) {
		return nil
	}
	if d.stopped {
		return nil
	}
	if msg.GetIsTombstone() {
		// we receive a message tells us to remove self.
		d.handleGCPeerMsg(msg)
		return nil
	}
	if d.checkMessage(msg) {
		return nil
	}
	key, err := d.checkSnapshot(msg)
	if err != nil {
		return err
	}
	if key != nil {
		// If the snapshot file is not used again, then it's OK to
		// delete them here. If the snapshot file will be reused when
		// receiving, then it will fail to pass the check again, so
		// missing snapshot files should not be noticed.
		s, err1 := d.ctx.snapMgr.GetSnapshotForApplying(*key)
		if err1 != nil {
			return err1
		}
		d.ctx.snapMgr.DeleteSnapshot(*key, s, false)
		return nil
	}
	d.insertPeerCache(msg.GetFromPeer())
	err = d.RaftGroup.Step(*msg.GetMessage())
	if err != nil {
		return err
	}
	if d.AnyNewPeerCatchUp(msg.FromPeer.Id) {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	return nil
}

// return false means the message is invalid, and can be ignored.
func (d *peerMsgHandler) validateRaftMessage(msg *rspb.RaftMessage) bool {
	regionID := msg.GetRegionId()
	from := msg.GetFromPeer()
	to := msg.GetToPeer()
	log.Debugf("[region %d] handle raft message %s from %d to %d", regionID, msg, from.GetId(), to.GetId())
	if to.GetStoreId() != d.storeID() {
		log.Warnf("[region %d] store not match, to store id %d, mine %d, ignore it",
			regionID, to.GetStoreId(), d.storeID())
		return false
	}
	if msg.RegionEpoch == nil {
		log.Errorf("[region %d] missing epoch in raft message, ignore it", regionID)
		return false
	}
	return true
}

/// Checks if the message is sent to the correct peer.
///
/// Returns true means that the message can be dropped silently.
func (d *peerMsgHandler) checkMessage(msg *rspb.RaftMessage) bool {
	fromEpoch := msg.GetRegionEpoch()
	isVoteMsg := util.IsVoteMessage(msg.Message)
	fromStoreID := msg.FromPeer.GetStoreId()

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with scheduler and scheduler will
	// tell 2 is stale, so 2 can remove itself.
	region := d.Region()
	if util.IsEpochStale(fromEpoch, region.RegionEpoch) && util.FindPeer(region, fromStoreID) == nil {
		// The message is stale and not in current region.
		handleStaleMsg(d.ctx.trans, msg, region.RegionEpoch, isVoteMsg)
		return true
	}
	target := msg.GetToPeer()
	if target.Id < d.PeerId() {
		log.Infof("%s target peer ID %d is less than %d, msg maybe stale", d.Tag, target.Id, d.PeerId())
		return true
	} else if target.Id > d.PeerId() {
		if d.MaybeDestroy() {
			log.Infof("%s is stale as received a larger peer %s, destroying", d.Tag, target)
			d.destroyPeer()
			d.ctx.router.sendStore(message.NewMsg(message.MsgTypeStoreRaftMessage, msg))
		}
		return true
	}
	return false
}

func handleStaleMsg(trans Transport, msg *rspb.RaftMessage, curEpoch *metapb.RegionEpoch,
	needGC bool) {
	regionID := msg.RegionId
	fromPeer := msg.FromPeer
	toPeer := msg.ToPeer
	msgType := msg.Message.GetMsgType()

	if !needGC {
		log.Infof("[region %d] raft message %s is stale, current %v ignore it",
			regionID, msgType, curEpoch)
		return
	}
	gcMsg := &rspb.RaftMessage{
		RegionId:    regionID,
		FromPeer:    toPeer,
		ToPeer:      fromPeer,
		RegionEpoch: curEpoch,
		IsTombstone: true,
	}
	if err := trans.Send(gcMsg); err != nil {
		log.Errorf("[region %d] send message failed %v", regionID, err)
	}
}

func (d *peerMsgHandler) handleGCPeerMsg(msg *rspb.RaftMessage) {
	fromEpoch := msg.RegionEpoch
	if !util.IsEpochStale(d.Region().RegionEpoch, fromEpoch) {
		return
	}
	if !util.PeerEqual(d.Meta, msg.ToPeer) {
		log.Infof("%s receive stale gc msg, ignore", d.Tag)
		return
	}
	log.Infof("%s peer %s receives gc message, trying to remove", d.Tag, msg.ToPeer)
	if d.MaybeDestroy() {
		d.destroyPeer()
	}
}

// Returns `None` if the `msg` doesn't contain a snapshot or it contains a snapshot which
// doesn't conflict with any other snapshots or regions. Otherwise a `snap.SnapKey` is returned.
func (d *peerMsgHandler) checkSnapshot(msg *rspb.RaftMessage) (*snap.SnapKey, error) {
	if msg.Message.Snapshot == nil {
		return nil, nil
	}
	regionID := msg.RegionId
	snapshot := msg.Message.Snapshot
	key := snap.SnapKeyFromRegionSnap(regionID, snapshot)
	snapData := new(rspb.RaftSnapshotData)
	err := snapData.Unmarshal(snapshot.Data)
	if err != nil {
		return nil, err
	}
	snapRegion := snapData.Region
	peerID := msg.ToPeer.Id
	var contains bool
	for _, peer := range snapRegion.Peers {
		if peer.Id == peerID {
			contains = true
			break
		}
	}
	if !contains {
		log.Infof("%s %s doesn't contains peer %d, skip", d.Tag, snapRegion, peerID)
		return &key, nil
	}
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	if !util.RegionEqual(meta.regions[d.regionId], d.Region()) {
		if !d.isInitialized() {
			log.Infof("%s stale delegate detected, skip", d.Tag)
			return &key, nil
		} else {
			panic(fmt.Sprintf("%s meta corrupted %s != %s", d.Tag, meta.regions[d.regionId], d.Region()))
		}
	}

	existRegions := meta.getOverlapRegions(snapRegion)
	for _, existRegion := range existRegions {
		if existRegion.GetId() == snapRegion.GetId() {
			continue
		}
		log.Infof("%s region overlapped %s %s", d.Tag, existRegion, snapRegion)
		return &key, nil
	}

	// check if snapshot file exists.
	_, err = d.ctx.snapMgr.GetSnapshotForApplying(key)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

func (d *peerMsgHandler) destroyPeer() {
	log.Infof("%s starts destroy", d.Tag)
	regionID := d.regionId
	// We can't destroy a peer which is applying snapshot.
	meta := d.ctx.storeMeta
	meta.Lock()
	defer meta.Unlock()
	isInitialized := d.isInitialized()
	if err := d.Destroy(d.ctx.engine, false); err != nil {
		// If not panic here, the peer will be recreated in the next restart,
		// then it will be gc again. But if some overlap region is created
		// before restarting, the gc action will delete the overlap region's
		// data too.
		panic(fmt.Sprintf("%s destroy peer %v", d.Tag, err))
	}
	d.ctx.router.close(regionID)
	d.stopped = true
	if isInitialized && meta.regionRanges.Delete(&regionItem{region: d.Region()}) == nil {
		panic(d.Tag + " meta corruption detected")
	}
	if _, ok := meta.regions[regionID]; !ok {
		panic(d.Tag + " meta corruption detected")
	}
	delete(meta.regions, regionID)
}

func (d *peerMsgHandler) findSiblingRegion() (result *metapb.Region) {
	meta := d.ctx.storeMeta
	meta.RLock()
	defer meta.RUnlock()
	item := &regionItem{region: d.Region()}
	meta.regionRanges.AscendGreaterOrEqual(item, func(i btree.Item) bool {
		result = i.(*regionItem).region
		return true
	})
	return
}

func (d *peerMsgHandler) onRaftGCLogTick() {
	d.ticker.schedule(PeerTickRaftLogGC)
	if !d.IsLeader() {
		return
	}

	appliedIdx := d.peerStorage.AppliedIndex()
	firstIdx, _ := d.peerStorage.FirstIndex()
	var compactIdx uint64
	if appliedIdx > firstIdx && appliedIdx-firstIdx >= d.ctx.cfg.RaftLogGcCountLimit {
		compactIdx = appliedIdx
	} else {
		return
	}

	y.Assert(compactIdx > 0)
	compactIdx -= 1
	if compactIdx < firstIdx {
		// In case compact_idx == first_idx before subtraction.
		return
	}

	term, err := d.RaftGroup.Raft.RaftLog.Term(compactIdx)
	if err != nil {
		log.Fatalf("appliedIdx: %d, firstIdx: %d, compactIdx: %d", appliedIdx, firstIdx, compactIdx)
		panic(err)
	}

	// Create a compact log request and notify directly.
	regionID := d.regionId
	request := newCompactLogRequest(regionID, d.Meta, compactIdx, term)
	d.proposeRaftCommand(request, nil)
}

func (d *peerMsgHandler) onSplitRegionCheckTick() {
	d.ticker.schedule(PeerTickSplitRegionCheck)
	// To avoid frequent scan, we only add new scan tasks if all previous tasks
	// have finished.
	if len(d.ctx.splitCheckTaskSender) > 0 {
		return
	}

	if !d.IsLeader() {
		return
	}
	if d.ApproximateSize != nil && d.SizeDiffHint < d.ctx.cfg.RegionSplitSize/8 {
		return
	}
	d.ctx.splitCheckTaskSender <- &runner.SplitCheckTask{
		Region: d.Region(),
	}
	d.SizeDiffHint = 0
}

func (d *peerMsgHandler) onPrepareSplitRegion(regionEpoch *metapb.RegionEpoch, splitKey []byte, cb *message.Callback) {
	if err := d.validateSplitRegion(regionEpoch, splitKey); err != nil {
		cb.Done(ErrResp(err))
		return
	}
	region := d.Region()
	d.ctx.schedulerTaskSender <- &runner.SchedulerAskSplitTask{
		Region:   region,
		SplitKey: splitKey,
		Peer:     d.Meta,
		Callback: cb,
	}
}

func (d *peerMsgHandler) validateSplitRegion(epoch *metapb.RegionEpoch, splitKey []byte) error {
	if len(splitKey) == 0 {
		err := errors.Errorf("%s split key should not be empty", d.Tag)
		log.Error(err)
		return err
	}

	if !d.IsLeader() {
		// region on this store is no longer leader, skipped.
		log.Infof("%s not leader, skip", d.Tag)
		return &util.ErrNotLeader{
			RegionId: d.regionId,
			Leader:   d.getPeerFromCache(d.LeaderId()),
		}
	}

	region := d.Region()
	latestEpoch := region.GetRegionEpoch()

	// This is a little difference for `check_region_epoch` in region split case.
	// Here we just need to check `version` because `conf_ver` will be update
	// to the latest value of the peer, and then send to Scheduler.
	if latestEpoch.Version != epoch.Version {
		log.Infof("%s epoch changed, retry later, prev_epoch: %s, epoch %s",
			d.Tag, latestEpoch, epoch)
		return &util.ErrEpochNotMatch{
			Message: fmt.Sprintf("%s epoch changed %s != %s, retry later", d.Tag, latestEpoch, epoch),
			Regions: []*metapb.Region{region},
		}
	}
	return nil
}

func (d *peerMsgHandler) onApproximateRegionSize(size uint64) {
	d.ApproximateSize = &size
}

func (d *peerMsgHandler) onSchedulerHeartbeatTick() {
	d.ticker.schedule(PeerTickSchedulerHeartbeat)

	if !d.IsLeader() {
		return
	}
	d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
}

func (d *peerMsgHandler) onGCSnap(snaps []snap.SnapKeyWithSending) {
	compactedIdx := d.peerStorage.truncatedIndex()
	compactedTerm := d.peerStorage.truncatedTerm()
	for _, snapKeyWithSending := range snaps {
		key := snapKeyWithSending.SnapKey
		if snapKeyWithSending.IsSending {
			snap, err := d.ctx.snapMgr.GetSnapshotForSending(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			if key.Term < compactedTerm || key.Index < compactedIdx {
				log.Infof("%s snap file %s has been compacted, delete", d.Tag, key)
				d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
			} else if fi, err1 := snap.Meta(); err1 == nil {
				modTime := fi.ModTime()
				if time.Since(modTime) > 4*time.Hour {
					log.Infof("%s snap file %s has been expired, delete", d.Tag, key)
					d.ctx.snapMgr.DeleteSnapshot(key, snap, false)
				}
			}
		} else if key.Term <= compactedTerm &&
			(key.Index < compactedIdx || key.Index == compactedIdx) {
			log.Infof("%s snap file %s has been applied, delete", d.Tag, key)
			a, err := d.ctx.snapMgr.GetSnapshotForApplying(key)
			if err != nil {
				log.Errorf("%s failed to load snapshot for %s %v", d.Tag, key, err)
				continue
			}
			d.ctx.snapMgr.DeleteSnapshot(key, a, false)
		}
	}
}

func newAdminRequest(regionID uint64, peer *metapb.Peer) *raft_cmdpb.RaftCmdRequest {
	return &raft_cmdpb.RaftCmdRequest{
		Header: &raft_cmdpb.RaftRequestHeader{
			RegionId: regionID,
			Peer:     peer,
		},
	}
}

func newCompactLogRequest(regionID uint64, peer *metapb.Peer, compactIndex, compactTerm uint64) *raft_cmdpb.RaftCmdRequest {
	req := newAdminRequest(regionID, peer)
	req.AdminRequest = &raft_cmdpb.AdminRequest{
		CmdType: raft_cmdpb.AdminCmdType_CompactLog,
		CompactLog: &raft_cmdpb.CompactLogRequest{
			CompactIndex: compactIndex,
			CompactTerm:  compactTerm,
		},
	}
	return req
}

// handle apply results from applyMsgHandler
func (d *peerMsgHandler) onApplyResult(res *message.MsgApplyResult) {
	d.peerStorage.applyState = res.ApplyState
	curSizeDiff := int64(d.SizeDiffHint) + res.SizeDiffHint
	if curSizeDiff > 0 {
		d.SizeDiffHint = uint64(curSizeDiff)
	} else {
		d.SizeDiffHint = 0
	}
	for _, r := range res.Results {
		switch r.ResType {
		case message.ResType_CompactLog:
			if d.LastCompactedIdx < r.CompactLogIndex {
				// Without this check,
				// it will print a lot of annoying 'no need to gc' infos.
				d.ScheduleCompactLog(r.CompactLogIndex)
			}
		case message.ResType_ChangePeer:
			d.onConfChangeRes(&r)
		case message.ResType_SplitRegion:
			d.onSplitRegionRes(&r)
		}
	}
}

func (d *peerMsgHandler) onConfChangeRes(r *message.ApplyResult) {
	// log.Infof("%s on config change result", d.Tag)
	// log.Infof("previous region: %s", d.Region())
	// log.Infof("new region: %s", r.newRegion)
	cc := r.ConfChange
	// d.updateRegion(r.newRegion)

	m := d.ctx.storeMeta
	m.Lock()
	m.regionRanges.Delete(&regionItem{region: d.Region()})
	m.setRegion(r.NewRegion, d.peer)
	m.regionRanges.ReplaceOrInsert(&regionItem{region: r.NewRegion})
	m.Unlock()

	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		if d.IsLeader() {
			d.PeersStartPendingTime[r.Peer.Id] = time.Now()
		}
		d.insertPeerCache(r.Peer)
	case pb.ConfChangeType_RemoveNode:
		if d.IsLeader() {
			delete(d.PeersStartPendingTime, r.Peer.Id)
		}
		d.removePeerCache(r.Peer.Id)
	}
	d.RaftGroup.ApplyConfChange(*cc)
	if d.IsLeader() {
		// log.Infof("%s notify heartbeat scheduler at %s", d.Tag, d.Region())
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
	}
	if cc.ChangeType == pb.ConfChangeType_RemoveNode &&
		cc.NodeId == d.PeerId() &&
		r.Peer.StoreId == d.storeID() {
		d.destroyPeer()
	}
}

func (d *peerMsgHandler) onSplitRegionRes(r *message.ApplyResult) {
	// notice: here d is still in the r.oldRegion
	// d.updateRegion(r.oldRegion)

	// see in onSplitRegionCheckTick
	// nil is meaningless, just as a trigger
	d.ApproximateSize = nil

	// must be atomic, or it will get stuck here
	m := d.ctx.storeMeta
	m.Lock()
	m.regionRanges.Delete(&regionItem{region: d.Region()})
	m.setRegion(r.OldRegion, d.peer)
	m.regions[r.NewRegion.Id] = r.NewRegion
	m.regionRanges.ReplaceOrInsert(&regionItem{region: r.OldRegion})
	m.regionRanges.ReplaceOrInsert(&regionItem{region: r.NewRegion})
	m.Unlock()

	// create new peers in new region
	newPeer, err := createPeer(d.ctx.store.Id, d.ctx.cfg,
		d.ctx.regionTaskSender, d.ctx.engine, r.NewRegion)
	if err != nil {
		log.Panic(err)
	}
	// log.Infof("%d create new peers %s", d.regionId, newPeer)
	for _, p := range r.NewRegion.Peers {
		newPeer.insertPeerCache(p)
	}
	// start workers, same in store_worker
	d.ctx.router.register(newPeer)
	_ = d.ctx.router.send(r.NewRegion.Id,
		message.NewPeerMsg(message.MsgTypeStart, r.NewRegion.Id, nil))
	// log.Infof("start worker")
	// leader should schedule heartbeat to update info
	if d.IsLeader() {
		d.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		// this peer is likely to become leader, send a heartbeat immediately.
		newPeer.HeartbeatScheduler(d.ctx.schedulerTaskSender)
		newPeer.MaybeCampaign(true)
	}
	// must handle the pending vote requests which are for the new peers
	m.Lock()
	restVotes := make([]*rspb.RaftMessage, 0)
	for _, v := range m.pendingVotes {
		if util.PeerEqual(v.ToPeer, newPeer.Meta) {
			_ = d.ctx.router.send(r.NewRegion.Id,
				message.NewPeerMsg(message.MsgTypeRaftMessage, r.NewRegion.Id, v))
		} else {
			restVotes = append(restVotes, v)
		}
	}
	m.pendingVotes = restVotes
	m.Unlock()
}
