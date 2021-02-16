package raft

import (
	"sort"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// doCommit is called when the leader needs to advance l.committed
// or make followers synchronized.
func (r *Raft) doCommit() bool {
	matchList := make(uint64Slice, 0)
	r.broadcast(func(id uint64) {
		matchList = append(matchList, r.Prs[id].Match)
	}, false)
	sort.Sort(matchList)
	mid := (len(r.Prs) - 1) / 2
	idx := matchList[mid]
	// commit to mid-idx, advance rl.committed
	rl := r.RaftLog
	// only log entries from the leader’s
	// current term are committed by counting replicas.
	if idx > rl.committed {
		logTerm, err := rl.Term(idx)
		if err != nil {
			log.Panic(err)
		}
		if logTerm == r.Term {
			rl.committed = idx
			return true
		}
	}
	return false
}

// handlePropose handle local Propose request
func (r *Raft) handlePropose(m pb.Message) error {
	entries := m.Entries
	log.Debugf("%d begin to propose %d entries", r.id, len(entries))
	// check valid id
	// check leadTransferee
	// check PendingConf
	lastIdx := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIdx + 1 + uint64(i)
	}
	r.RaftLog.appendEntries(entries)
	lastIdx = r.RaftLog.LastIndex()
	// update self-progress
	pr := r.Prs[r.id]
	if lastIdx > pr.Match {
		pr.Match = lastIdx
		pr.Next = lastIdx + 1
	}
	r.doCommit()
	r.broadcast(func(id uint64) { r.sendAppend(id) }, true)
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	rl := r.RaftLog
	logTerm, err := rl.Term(m.Index)
	if err != nil || logTerm != m.LogTerm { //- mismatch term
		i := min(m.Index, rl.LastIndex())
		for ; i > rl.committed; i-- {
			term, err := rl.Term(i)
			if err != nil || term != logTerm {
				break
			}
		}
		r.sendAppendResponse(m.From, true, i)
		return
	}
	//- find conflict index
	for i, entry := range m.Entries {
		term, err := rl.Term(entry.Index)
		if err != nil || term != entry.Term {
			rl.appendEntries(m.Entries[i:])
		}
	}
	toCommit := min(m.Commit, m.Index+uint64(len(m.Entries)))
	if toCommit > rl.committed {
		rl.committed = toCommit
	}
	r.sendAppendResponse(m.From, false, rl.LastIndex())
}

// handleAppendResponse handle AppendResponse RPC response
func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	spr := r.sPrs[m.From]
	if m.Reject {
		// non-leader node refused to append
		// leader could bring a follower's log
		// into consistency with its own.
		if m.Index >= pr.Match {
			pr.Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}
	//- synchronized
	if m.Index > pr.Match {
		pr.Match = m.Index
		pr.Next = m.Index + 1
		if r.doCommit() {
			r.broadcast(func(id uint64) { r.sendAppend(id) }, true)
		}
	}
	if pr.Match > spr.pendingIdx {
		spr.state = StateNormal
		spr.pendingIdx = 0
		spr.resendTick = 0
	}
	// leadTransferee
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	rl := r.RaftLog
	if rl.committed < m.Commit {
		rl.committed = m.Commit
	}
	r.sendHeartbeatResponse(m.From)
}

// handleHeartbeatResponse handle HeartbeatResponse RPC response
func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if pr.Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleVoteRequest handle RequestVote RPC request
func (r *Raft) handleVoteRequest(m pb.Message) {
	// each follower will vote for at most one
	// candidate in a given term, on a offset-come-offset-served basis.
	// the state of the requester must be beyond the responser.
	isBeyond := (m.LogTerm > r.RaftLog.LastTerm()) ||
		(m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())
	// If the responser has voted another one or is beyond the requester, it cannot vote.
	canVote := ((r.Vote == None && r.Lead == None) || (r.Vote == m.From)) && isBeyond
	if canVote {
		r.Vote = m.From
	}
	r.sendVoteResponse(m.From, !canVote)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
	s := m.Snapshot
	sm := s.Metadata
	sIdx, sTerm := sm.Index, sm.Term
	// check term
	term, _ := r.RaftLog.Term(sIdx)
	if term == sTerm {
		r.sendAppendResponse(m.From, false, r.RaftLog.committed)
		return
	}
	r.RaftLog.storeSnap(s)

	// update conf
	lastIdx := r.RaftLog.LastIndex()
	r.Prs = make(map[uint64]*Progress)
	for _, id := range sm.ConfState.Nodes {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  lastIdx + 1,
		}
		r.sPrs[id] = &SnapProgress{}
	}
	r.Prs[r.id].Match = lastIdx

	r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
