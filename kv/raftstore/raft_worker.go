package raftstore

import (
	"sync"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
)

// raftWorker is responsible for run raft commands and apply raft logs.
type raftWorker struct {
	pr *router

	// receiver of messages should sent to raft, including:
	// * raft command from `raftStorage`
	// * raft inner messages from other peers sent by network
	raftCh chan message.Msg
	ctx    *GlobalContext

	applyCh chan []message.Msg

	closeCh <-chan struct{}
}

func newRaftWorker(ctx *GlobalContext, pm *router) *raftWorker {
	return &raftWorker{
		raftCh:  pm.peerSender,
		ctx:     ctx,
		applyCh: make(chan []message.Msg, 40960),
		pr:      pm,
	}
}

// run runs raft commands.
// On each loop, raft commands are batched by channel buffer.
// After commands are handled, we collect apply messages by peers, make a applyBatch, send it to apply channel.
func (rw *raftWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	var msgs []message.Msg
	for {
		msgs = msgs[:0]
		select {
		case <-closeCh:
			return
		case msg := <-rw.raftCh:
			msgs = append(msgs, msg)
		}
		pending := len(rw.raftCh)
		for i := 0; i < pending; i++ {
			msgs = append(msgs, <-rw.raftCh)
		}
		peerStateMap := make(map[uint64]*peerState)
		for _, msg := range msgs {
			peerState := rw.getPeerState(peerStateMap, msg.RegionID)
			if peerState == nil {
				continue
			}
			newPeerMsgHandler(peerState.peer, rw.applyCh, rw.ctx).HandleMsg(msg)
		}
		for _, peerState := range peerStateMap {
			newPeerMsgHandler(peerState.peer, rw.applyCh, rw.ctx).HandleRaftReady()
		}
	}
}

func (rw *raftWorker) getPeerState(peersMap map[uint64]*peerState, regionID uint64) *peerState {
	peer, ok := peersMap[regionID]
	if !ok {
		peer = rw.pr.get(regionID)
		if peer == nil {
			return nil
		}
		peersMap[regionID] = peer
	}
	return peer
}

// applyWorker is only responsible for apply committed entries.
// It gets msgs from raftWorker through applyCh.
// Its implementation is similar with raftWorker.
type applyWorker struct {
	pr  *router
	ctx *GlobalContext

	applyCh chan []message.Msg
	closeCh <-chan struct{}

	aCtx *applyContext
}

func newApplyWorker(ctx *GlobalContext, applyCh chan []message.Msg, pr *router) *applyWorker {
	return &applyWorker{
		pr:      pr,
		applyCh: applyCh,
		ctx:     ctx,
		aCtx:    newApplyContext(ctx.engine, pr),
	}
}

func (aw *applyWorker) run(closeCh <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-closeCh:
			return
		case msgs := <-aw.applyCh:
			for _, msg := range msgs {
				// handle apply
				ps := aw.pr.get(msg.RegionID)
				if ps == nil {
					continue
				}
				newApplyMsgHandler(ps.applier, aw.aCtx).handleApplyMsg(msg)
			}
		}
	}
}
