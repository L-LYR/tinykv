package raftstore

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

// applier save the basic infos of peer for applyMsgHandler
type applier struct {
	id      uint64
	term    uint64
	region  *metapb.Region
	tag     string
	cmds    []*message.ApplyCmd
	removed bool
}

func newApplier(peer *peer) *applier {
	return &applier{
		id:      peer.PeerId(),
		term:    peer.Term(),
		region:  peer.Region(),
		removed: false,
		cmds:    nil,
		tag:     peer.Tag, // the same tag with peer
	}
}

// if there is a snapshot applied, applier must update its infos
// and clear all stale cmds.
func (a *applier) update(m *message.MsgApply) {
	if a.id != m.Id {
		log.Panicf("%d != %d", a.id, m.Id)
	}
	a.term = m.Term
	a.clear()
	a.tag = fmt.Sprintf("[region %d] %d", m.Region.Id, m.Id)
	a.region = m.Region
}

// clear will clear up all the stale requests.
func (a *applier) clear() {
	for _, p := range a.cmds {
		NotifyStaleReq(a.term, p.Cb)
	}
	a.cmds = nil
}

func (a *applier) appendCmds(ps []*message.ApplyCmd) {
	a.cmds = append(a.cmds, ps...)
}

// find the corresponding callback for a certain entry
// if not found, just return nil.
func (a *applier) getCallback(index, term uint64) *message.Callback {
	for {
		if len(a.cmds) == 0 {
			return nil
		}
		p := a.cmds[0]
		a.cmds = a.cmds[1:]
		// mismatch
		if term < p.Term {
			return nil
		}
		// match
		if p.Term == term && p.Index == index {
			return p.Cb
		}
		NotifyStaleReq(p.Term, p.Cb)
	}
}

func (a *applier) destroy() {
	regid := a.region.Id
	log.Infof("%s applier destroyed", a.tag)
	for _, p := range a.cmds {
		NotifyReqRegionRemoved(regid, p.Cb)
	}
	a.cmds = nil
	a.region = nil
}
