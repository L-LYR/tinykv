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
	if !r.isValidID(r.id) {
		log.Debugf("%d has been removed", r.id)
		return ErrProposalDropped
	}

	if r.leadTransferee != None {
		log.Debugf("leader is transferring from %d to %d", r.id, r.leadTransferee)
		return ErrProposalDropped
	}

	entries := m.Entries
	log.Debugf("%d begin to propose %d entries", r.id, len(entries))
	lastIdx := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = lastIdx + 1 + uint64(i)
		if entries[i].EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				entries[i].EntryType = pb.EntryType_EntryNormal
				entries[i].Data = nil
			} else {
				r.PendingConfIndex = entries[i].Index
			}
		}
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
			break
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
	spr.alive = true
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
	if r.leadTransferee == m.From && pr.Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(r.leadTransferee)
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
	spr := r.sPrs[m.From]
	spr.alive = true
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

// handleSnapshot handle TransferLeader RPC request
func (r *Raft) handleTransferLeader(m pb.Message) {
	if !r.isValidID(m.From) {
		// invalid id
		return
	}
	if r.State == StateFollower {
		m.To = r.Lead
		r.send(m)
		return
	}
	if m.From == r.id {
		// self-transfer
		return
	}
	if r.leadTransferee != None && m.From == r.leadTransferee {
		// pending
		return
	}
	r.leadTransferee = m.From
	r.transferLeaderElapsed = 0
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From)
	}
}

// isValidID checks the id
func (r *Raft) isValidID(id uint64) bool {
	if _, has := r.Prs[id]; !has {
		return false
	}
	return true
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
	if !r.isValidID(id) {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
		r.sPrs[id] = &SnapProgress{
			state:      StateNormal,
			resendTick: 0,
			pendingIdx: 0,
			alive:      true,
		}
	}
	//var prsList []string
	//for i := range r.Prs {
	//prsList = append(prsList, fmt.Sprint(i))
	//}
	//log.Infof("%d after add %d, current peers: %s, leader: %d", r.id, id, strings.Join(prsList, ","), r.Lead)
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
	if !r.isValidID(id) {
		return
	}
	delete(r.Prs, id)
	delete(r.sPrs, id)

	if r.id != id && r.State == StateLeader {
		if r.doCommit() {
			r.broadcast(func(id uint64) { r.sendAppend(id) }, true)
		}
		if r.leadTransferee == id {
			r.leadTransferee = None
			r.transferLeaderElapsed = 0
		}
	}
	//var prsList []string
	//for i := range r.Prs {
	//prsList = append(prsList, fmt.Sprint(i))
	//}
	//log.Infof("%d after remove %d, current peers: %s, leader: %d", r.id, id, strings.Join(prsList, ","), r.Lead)
}
