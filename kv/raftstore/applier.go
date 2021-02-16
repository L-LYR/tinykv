package raftstore

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type applier struct {
	id        uint64
	term      uint64
	region    *metapb.Region
	tag       string
	proposals []*proposal
}

func newApplier(peer *peer) *applier {
	return &applier{
		id:     peer.PeerId(),
		term:   peer.Term(),
		region: peer.Region(),
		tag:    fmt.Sprintf("[region %d] %d", peer.regionId, peer.PeerId()), // the same tag with peer
	}
}

func (a *applier) appendProposals(ps []*proposal) {
	a.proposals = append(a.proposals, ps...)
}

func (a *applier) getCallback(index, term uint64) *message.Callback {
	for {
		if len(a.proposals) == 0 {
			return nil
		}
		p := a.proposals[0]
		a.proposals = a.proposals[1:]
		// mismatch
		if term < p.term {
			return nil
		}
		// match
		if p.term == term && p.index == index {
			return p.cb
		}
		NotifyStaleReq(p.term, p.cb)
	}
}

func (a *applier) destroy() {
	regid := a.region.Id
	log.Infof("%s applier destroyed", a.tag)
	for _, p := range a.proposals {
		NotifyReqRegionRemoved(regid, p.cb)
	}
	a.proposals = nil
	a.region = nil
}
