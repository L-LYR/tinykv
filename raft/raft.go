// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"math/rand"
	"sort"

	"github.com/pingcap/log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
//- The leader maintains a nextIndex foreach follower,
//- which is the index of the next log entry the leader will
//- send to that follower.
//- But here we need to init these in all nodes
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed  int
	randElectionTime int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	//! Your Code Here (2A).
	//- generate empty peerID -> Progress map from config
	prs := make(map[uint64]*Progress)
	lastIdx, _ := c.Storage.LastIndex()
	for _, p := range c.peers {
		if p == c.ID {
			prs[p] = &Progress{Next: lastIdx + 1, Match: lastIdx}
		} else {
			prs[p] = &Progress{Next: lastIdx + 1}
		}
	}
	//? When to use confState
	hardState, _, _ := c.Storage.InitialState()
	rLog := newLog(c.Storage)
	rLog.committed = hardState.Commit
	//- init a new raft peer
	return &Raft{
		id:      c.ID,
		Term:    hardState.GetTerm(),
		Vote:    hardState.GetVote(),
		RaftLog: rLog,
		Prs:     prs,
		//- raft_paper_test.go:76:TestStartAsFollower2AA
		State:            StateFollower, // start as follower
		votes:            make(map[uint64]bool),
		msgs:             make([]pb.Message, 0),
		Lead:             None,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		randElectionTime: c.ElectionTick + rand.Intn(c.ElectionTick),
		electionElapsed:  0,
		heartbeatElapsed: 0,
		// Used in 3A
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
}

func (r *Raft) lim() int { return len(r.Prs) / 2 }

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	//! Your Code Here (2A).
	prevIdx := r.Prs[to].Next - 1
	prevTerm, err := r.RaftLog.Term(prevIdx)
	if err != nil {
		log.Panic(err.Error())
	}
	entps := make([]*pb.Entry, 0)
	ents := r.RaftLog.Entries(prevIdx, r.RaftLog.LastIndex()+1)
	for i := range ents {
		entps = append(entps, &ents[i])
	}

	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
		Index:   prevIdx,
		LogTerm: prevTerm,
		Entries: entps,
	}
	r.msgs = append(r.msgs, m)
	return true
}

// sendAppendResponse sends a append entries response to the given peer
func (r *Raft) sendAppendResponse(to uint64, reject bool, logTerm uint64, index uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		LogTerm: logTerm,
		Index:   index,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, m)
}

// sendVoteRequest sends a vote request to the given peer
func (r *Raft) sendVoteRequest(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	}
	r.msgs = append(r.msgs, m)
}

// sendVoteResponse sends a vote response to the given peer
func (r *Raft) sendVoteResponse(to uint64, reject bool) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, m)
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	//! Your Code Here (2A).
	//- If the leader receives a heartbeat tick,
	//- it will send a MessageType_MsgHeartbeat
	//- with m.Index = 0, m.LogTerm=0 and empty entries
	//- as heartbeat to all followers.
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		//- TODO: reference
		Commit: min(r.RaftLog.committed, r.Prs[to].Match),
	}
	r.msgs = append(r.msgs, m)
}

// sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64, reject bool) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, m)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	//! Your Code Here (2A).
	//- Only leader keeps heartbeatElapsed.
	if r.State == StateLeader {
		r.tickHeartbeat()
	} else {
		r.tickElection()
	}
}

// tickElection only for candidates and followers
func (r *Raft) tickElection() {
	r.electionElapsed++
	//- The follower meets a electionTimeout and raises a new election.
	if r.electionElapsed >= r.randElectionTime {
		r.electionElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			From:    r.id,
		})
	}
}

// tickHeartbeat only for the leader
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			To:      r.id,
		})
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	//! Your Code Here (2A).
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None //- haven't voted after becoming follower
	r.votes = make(map[uint64]bool)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	//! Your Code Here (2A).
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id                   //- vote myself
	r.votes = make(map[uint64]bool) //- remaking is quicker than clearing the old one
	r.votes[r.id] = true
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	//! Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None //- haven't voted after becoming follower
	r.votes = make(map[uint64]bool)
	//- append a noop entry
	lastIdx := r.RaftLog.LastIndex()
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{
		Term:  r.Term,
		Index: lastIdx + 1,
		Data:  nil,
	})
	//- broadcast noop entry
	for p := range r.Prs {
		if p != r.id {
			r.Prs[p].Next = lastIdx + 1
			r.sendAppend(p)
		} else {
			r.Prs[p].Next = lastIdx + 2
			r.Prs[p].Match = lastIdx + 1
		}
	}
	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.Prs[r.id].Match
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) (err error) {
	//! Your Code Here (2A).
	if m.Term > r.Term {
		//- test_paper_test.go:52:testUpdateTermFromMessage
		if m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	}

	switch r.State {
	case StateFollower:
		err = r.stepFollower(m)
	case StateCandidate:
		err = r.stepCandidate(m)
	case StateLeader:
		err = r.stepLeader(m)
	}
	return
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		//- If the leader receives a heartbeat tick,
		//- it will send a MessageType_MsgHeartbeat
		//- with m.Index = 0, m.LogTerm=0 and empty entries
		//- as heartbeat to all followers.
		//- raft_paper_test.go:87:TestLeaderBcastBeat2AA
		for pid := range r.Prs {
			if r.id != pid {
				r.sendHeartbeat(pid)
			}
		}
	case pb.MessageType_MsgPropose:
		//- When receiving client proposals,
		//- the leader appends the proposal to its log as a new entry, then issues
		//- AppendEntries RPCs in parallel to each of the other servers to replicate
		//- the entry. Also, when sending an AppendEntries RPC, the leader includes
		//- the index and term of the entry in its log that immediately precedes
		//- the new entries.
		//- Also, it writes the new entry into stable storage.
		r.handleProposal(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection(m)
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection(m)
	case pb.MessageType_MsgAppend:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.getVote(m.From, m.Reject)
	case pb.MessageType_MsgHeartbeat:
		if m.Term == r.Term {
			r.becomeFollower(m.Term, m.From)
		}
		r.handleHeartbeat(m)
	}
	return nil
}

// doElection begins a new election by broadcasting vote requests
func (r *Raft) doElection(m pb.Message) {
	r.heartbeatElapsed = 0
	r.heartbeatElapsed = 0
	r.randElectionTime = r.electionTimeout + rand.Intn(r.electionTimeout)
	r.becomeCandidate()
	//- only one peer
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendVoteRequest(p)
		}
	}
}

// doCommit updates the leader's committed index
func (r *Raft) doCommit() {
	//- util 113
	curMatches := make(uint64Slice, 0)
	for _, p := range r.Prs {
		curMatches = append(curMatches, p.Match)
	}
	sort.Sort(curMatches)
	mid := (len(r.Prs) - 1) / 2
	midIdx := curMatches[mid]
	logTerm, err := r.RaftLog.Term(midIdx)
	if err != nil {
		log.Panic(err.Error())
	}
	// only log entries from the leader’s
	// current term are committed by counting replicas.
	if logTerm == r.Term && midIdx > r.RaftLog.committed {
		r.RaftLog.committed = midIdx
		for p := range r.Prs {
			if p != r.id {
				r.sendAppend(p)
			}
		}
	}
}

// getVote gets vote from one node and re-count the votes
func (r *Raft) getVote(from uint64, reject bool) {
	r.votes[from] = !reject
	gain, lose := 0, 0
	for _, pv := range r.votes {
		if pv {
			gain++
		} else {
			lose++
		}
	}
	if gain > r.lim() {
		r.becomeLeader()
		return
	}
	if lose > r.lim() {
		r.becomeFollower(r.Term, None)
		return
	}
}

// handleVoteRequest handle vote request
func (r *Raft) handleVoteRequest(m pb.Message) {
	//- handle the behind request
	if m.Term < r.Term {
		r.sendVoteResponse(m.From, true)
		return
	}
	//- the state of the requester must be beyond the responser.
	isBeyond := (m.LogTerm > r.RaftLog.LastTerm()) ||
		(m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())
	//- If the responser has voted another one or is beyond the requester, it cannot vote.
	canVote := ((r.Vote == None && r.Lead == None) || (r.Vote == m.From)) && isBeyond
	if canVote {
		r.Vote = m.From
	}
	r.sendVoteResponse(m.From, !canVote)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	//! Your Code Here (2A).
	if m.Term < r.Term {
		//- reject the behind request for appending entries
		r.sendAppendResponse(m.From, true, None, None)
		return
	}
	lastIdx := r.RaftLog.LastIndex()
	if m.Index > lastIdx {
		//- the sender's index is greater than the receiver's lastIndex
		r.sendAppendResponse(m.From, true, None, lastIdx+1)
		return
	}
	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil {
		log.Panic(err.Error())
	}
	// If the follower does not find an
	// entry in its log with the same index and term as the one in AppendEntries RPC,
	// then it refuses the new entries. Otherwise it replies that it accepts the
	// append entries.
	// raft_paper_test.go:571:TestFollowerCheckMessageType_MsgAppend2AB
	if logTerm != m.LogTerm {
		//- find the respective index and reject
		actualIdx := r.RaftLog.offset - 1
		for i := uint64(0); i < m.Index-r.RaftLog.offset+1; i++ {
			if r.RaftLog.entries[i].Term == logTerm {
				actualIdx = i + r.RaftLog.offset
				break
			}
		}
		r.sendAppendResponse(m.From, true, logTerm, actualIdx)
		return
	}
	//- append entries in m
	// When AppendEntries RPC is valid,
	// the follower will delete the existing conflict entry and all that follow it,
	// and append any new entries not already in the log.
	// Also, it writes the new entry into stable storage.
	// raft_paper_test.go:617:TestFollowerAppendEntries2AB
	for i, entry := range m.Entries {
		if entry.Index > r.RaftLog.LastIndex() {
			//- append all new entries
			for ; i < len(m.Entries); i++ {
				r.RaftLog.entries = append(r.RaftLog.entries, *m.Entries[i])
			}
			break
		} else {
			logTerm, err := r.RaftLog.Term(entry.Index)
			if err != nil {
				log.Panic(err.Error())
			}
			if logTerm != entry.Term {
				//- resolve conflict
				idx := entry.Index - r.RaftLog.offset
				r.RaftLog.entries[idx] = *entry
				r.RaftLog.entries = r.RaftLog.entries[:idx+1]
				if r.RaftLog.stabled >= entry.Index {
					r.RaftLog.stabled = entry.Index - 1
				}
			}
			//- just the same, so do nothing
		}
	}
	// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry).
	if m.Commit > r.RaftLog.committed {
		//r.RaftLog.committed = min(m.Commit, r.RaftLog.LastIndex())
		r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
	}
	r.sendAppendResponse(m.From, false, None, r.RaftLog.LastIndex())
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term < r.Term {
		//- do nothing for behind response
		return
	}

	if m.Reject {
		//- non-leader node refused to append
		// leader could bring a follower's log
		// into consistency with its own.
		actualIdx := -1
		for i := 0; i < len(r.RaftLog.entries); i++ {
			if r.RaftLog.entries[i].Term > m.LogTerm {
				actualIdx = i
				break
			}
		}
		if actualIdx > 0 && r.RaftLog.entries[actualIdx-1].Term == m.LogTerm {
			r.Prs[m.From].Next = uint64(actualIdx) + r.RaftLog.offset
		} else {
			r.Prs[m.From].Next = m.Index
		}
		r.sendAppend(m.From)
		return
	}

	r.Prs[m.From].Match = m.Index
	r.Prs[m.From].Next = m.Index + 1
	// leader commit
	r.doCommit()
}

func (r *Raft) handleProposal(m pb.Message) {
	lastIdx := r.RaftLog.LastIndex()
	for i, entry := range m.Entries {
		entry.Term = r.Term
		entry.Index = lastIdx + uint64(i) + 1
		r.RaftLog.entries = append(r.RaftLog.entries, *entry)
	}
	lastIdx = r.RaftLog.LastIndex()
	r.Prs[r.id].Match = lastIdx
	r.Prs[r.id].Next = lastIdx + 1
	for p := range r.Prs {
		if p != r.id {
			r.sendAppend(p)
		}
	}
	//- If there is only one node, just commit this entry.
	if len(r.Prs) == 1 {
		r.RaftLog.committed = lastIdx
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	//! Your Code Here (2A).
	//- handle the behind heartbeat
	if m.Term < r.Term {
		r.sendHeartbeatResponse(m.From, true)
		return
	}
	r.RaftLog.committed = m.Commit
	r.sendHeartbeatResponse(m.From, false)
	r.becomeFollower(m.Term, m.From)
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
