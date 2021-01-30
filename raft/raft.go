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
	"fmt"
	"math/rand"
	"sort"
	"strings"

	"github.com/pingcap-incubator/tinykv/log"

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
		panic(err)
	}
	//! Your Code Here (2A).
	//- ConfState used in 2B
	hardState, confState, err := c.Storage.InitialState()
	if err != nil {
		panic(err)
	}
	rLog := newLog(c.Storage)
	rLog.committed = hardState.Commit
	if c.Applied > 0 {
		//- Applied is the last applied index. It should only be set when restarting
		//- raft. raft will not return entries to the application smaller or equal to
		//- Applied. If Applied is unset when restarting, raft might return previous
		//- applied entries. This is a very application dependent configuration.
		rLog.applied = c.Applied
	}
	peers := confState.Nodes
	if len(c.peers) != 0 {
		peers = c.peers
	}

	prs := make(map[uint64]*Progress)
	//- generate empty peerID -> Progress map from config
	for _, p := range peers {
		prs[p] = &Progress{Next: 1}
	}
	//- init a new raft peer
	r := Raft{
		id:      c.ID,
		Term:    hardState.GetTerm(),
		Vote:    hardState.GetVote(),
		RaftLog: rLog,
		Prs:     prs,
		//- raft_paper_test.go:76:TestStartAsFollower2AA
		State: StateFollower, // start as follower
		votes: make(map[uint64]bool),
		//msgs:             make([]pb.Message, 0),
		//- If here use make() to initialize the msgs, it will cause
		//- strange errors when using reflect.DeepEqual() to check.
		//- reflect.DeepEqual() returns false if one slice is nil,
		//- and the other is a non-nil slice with 0 length.
		msgs:             nil,
		Lead:             None,
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		randElectionTime: c.ElectionTick + rand.Intn(c.ElectionTick),
		electionElapsed:  0,
		heartbeatElapsed: 0,
		leadTransferee:   0,
		PendingConfIndex: 0,
	}
	var prss []string
	for _, p := range peers {
		prss = append(prss, fmt.Sprint(p))
	}
	log.Debugf("newRaft %d [peers: (%s), term: %d, first: %d, applied: %d, committed: %d, stabled: %d, last: %d]\n",
		r.id, strings.Join(prss, ","), r.Term, r.RaftLog.FirstIndex(), r.RaftLog.applied,
		r.RaftLog.committed, r.RaftLog.stabled, r.RaftLog.LastIndex())
	return &r
}

// curSoftState returns the current soft state of raft
func (r *Raft) curSoftState() *SoftState {
	return &SoftState{
		Lead:      r.Lead,
		RaftState: r.State,
	}
}

// curHardState returns the current hard state of raft
func (r *Raft) curHardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.RaftLog.committed,
	}
}

// lim returns the threshold of winning the election
func (r *Raft) lim() int { return len(r.Prs) / 2 }

// isValidID checks the id
func (r *Raft) isValidID(id uint64) bool {
	if _, has := r.Prs[id]; !has {
		return false
	}
	return true
}

// broadcast does the broadcast for the function like sendAppend and sendHeartbeat
func (r *Raft) broadcast(fn func(id uint64)) {
	for p := range r.Prs {
		fn(p)
	}
}

var showList = map[pb.MessageType]bool{
	pb.MessageType_MsgSnapshot: true,
}

func (r *Raft) send(msg pb.Message) {
	if _, ok := showList[msg.MsgType]; ok {
		log.Debugf("%d send %v to %d at term %d with Index %d",
			msg.From, msg.MsgType, msg.To, msg.Term, msg.Index)
	}
	r.msgs = append(r.msgs, msg)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	//! Your Code Here (2A).
	if to == r.id {
		return false
	}
	prevIdx := r.Prs[to].Next - 1
	prevTerm, getTermErr := r.RaftLog.Term(prevIdx)
	ents, getEntriesErr := r.RaftLog.Entries(r.Prs[to].Next)
	if getTermErr != nil || getEntriesErr != nil {
		return r.sendSnapShot(to)
	}
	entps := make([]*pb.Entry, 0, len(ents))
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
	r.send(m)
	return true
}

// sendAppendResponse sends a append entries response to the given peer
func (r *Raft) sendAppendResponse(to uint64, reject bool, index uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   index,
		Reject:  reject,
	}
	r.send(m)
}

// sendVoteRequest sends a vote request to the given peer
func (r *Raft) sendVoteRequest(to uint64) {
	if to == r.id {
		return
	}
	m := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   r.RaftLog.LastIndex(),
		LogTerm: r.RaftLog.LastTerm(),
	}
	r.send(m)
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
	r.send(m)
}

// sendSnapShot sends a snapshot
func (r *Raft) sendSnapShot(to uint64) bool {
	snapshot, err := r.RaftLog.snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			return false
		}
		panic(err)
	}
	if IsEmptySnap(&snapshot) {
		panic("Empty Snapshot!")
	}
	m := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		From:     r.id,
		To:       to,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.send(m)
	return true
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	//! Your Code Here (2A).
	//- If the leader receives a heartbeat tick,
	//- it will send a MessageType_MsgHeartbeat
	//- with m.Index = 0, m.LogTerm=0 and empty entries
	//- as heartbeat to all followers.
	if to == r.id {
		return
	}
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.RaftLog.committed, r.Prs[to].Match),
	}
	r.send(m)
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
	r.send(m)
}

// send
func (r *Raft) sendTimeoutNow(to uint64) {
	r.leadTransferee = None
	m := pb.Message{
		MsgType: pb.MessageType_MsgTimeoutNow,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.send(m)
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

// resetInfo will be called in state changes
func (r *Raft) resetInfo() {
	r.heartbeatElapsed = 0
	r.heartbeatElapsed = 0
	r.randElectionTime = r.electionTimeout + rand.Intn(r.electionTimeout)

	r.PendingConfIndex = 0
	r.leadTransferee = 0
	r.votes = make(map[uint64]bool)

}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	//! Your Code Here (2A).
	r.resetInfo()
	r.State = StateFollower
	r.Lead = lead
	r.Term = term
	r.Vote = None //- haven't voted after becoming follower
	log.Debugf("%d became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	//! Your Code Here (2A).
	r.resetInfo()
	r.State = StateCandidate
	r.Lead = None
	r.Term++
	r.Vote = r.id //- vote myself
	r.votes[r.id] = true
	log.Debugf("%d became candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	//! Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	//- I think the NOTE is wrong
	//- In the project2-RaftKV.md, the hints say
	//- The tests assume that the new elected leader
	//- should *append* a noop entry on its term.
	r.resetInfo()
	r.State = StateLeader
	r.Lead = r.id
	r.Vote = None
	// pendingConfIndex
	r.PendingConfIndex = r.RaftLog.LastIndex()
	lastIdx := r.RaftLog.LastIndex()
	r.broadcast(func(id uint64) {
		r.Prs[id].Next = lastIdx + 1
		r.Prs[id].Match = 0
	})
	r.Prs[r.id].Match = lastIdx
	//- broadcast noop entry by proposal
	r.Step(pb.Message{MsgType: pb.MessageType_MsgPropose, Entries: []*pb.Entry{
		{Data: nil},
	}})
	//r.appendEntries([]*pb.Entry{{Data: nil}})
	log.Debugf("%d became leader at term %d", r.id, r.Term)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) (err error) {
	//! Your Code Here (2A).
	if m.Term == 0 {
		//-	local message
	} else if m.Term > r.Term {
		//- test_paper_test.go:52:testUpdateTermFromMessage
		if m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	} else if m.Term < r.Term {
		switch m.MsgType {
		case pb.MessageType_MsgRequestVote:
			r.sendVoteResponse(m.From, true)
		case pb.MessageType_MsgHeartbeat:
			r.sendHeartbeatResponse(m.From, true)
		case pb.MessageType_MsgAppend:
			r.sendAppendResponse(m.From, true, None)
		case pb.MessageType_MsgSnapshot:
			r.sendAppendResponse(m.From, true, None)
		}
		return nil
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
		r.broadcast(r.sendHeartbeat)
	case pb.MessageType_MsgPropose:
		//- When receiving client proposals,
		//- the leader appends the proposal to its log as a new entry, then issues
		//- AppendEntries RPCs in parallel to each of the other servers to replicate
		//- the entry. Also, when sending an AppendEntries RPC, the leader includes
		//- the index and term of the entry in its log that immediately precedes
		//- the new entries.
		//- Also, it writes the new entry into stable storage.
		if r.leadTransferee != None {
			return ErrProposalDropped
		}
		r.handleProposal(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgSnapshot:
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.Lead = m.From
		r.electionElapsed = 0
		r.handleHeartbeat(m)
	case pb.MessageType_MsgTransferLeader:
		r.handleTransferLeader(m)
	case pb.MessageType_MsgTimeoutNow:
		r.doElection()
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.doElection()
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.getVote(m.From, m.Reject)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	}
	return nil
}

// doElection begins a new election by broadcasting vote requests
func (r *Raft) doElection() {
	if !r.isValidID(r.id) {
		return
	}
	r.becomeCandidate()
	//- only one peer
	if len(r.Prs) == 1 {
		r.becomeLeader()
		return
	}
	r.broadcast(r.sendVoteRequest)
}

// doCommit updates the leader's committed index
func (r *Raft) doCommit() bool {
	//- util 113
	curMatches := make(uint64Slice, 0, len(r.Prs))
	for _, p := range r.Prs {
		curMatches = append(curMatches, p.Match)
	}
	sort.Sort(curMatches)
	mid := len(r.Prs) - r.lim() - 1
	idx := curMatches[mid]
	// only log entries from the leader’s
	// current term are committed by counting replicas.
	if idx > r.RaftLog.committed {
		logTerm, err := r.RaftLog.Term(idx)
		if err != nil {
			panic(err)
		}
		if logTerm == r.Term {
			r.RaftLog.committed = idx
			return true
		}
	}
	return false
}

// getVote gets vote from one node and re-count the votes
func (r *Raft) getVote(from uint64, reject bool) {
	if _, ok := r.votes[from]; !ok {
		r.votes[from] = !reject
	}
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
	//- the state of the requester must be beyond the responser.
	//fmt.Printf("m: %d %d\n", m.LogTerm, m.Index)
	//fmt.Printf("r: %d %d\n", r.RaftLog.LastTerm(), r.RaftLog.LastIndex())
	isBeyond := (m.LogTerm > r.RaftLog.LastTerm()) ||
		(m.LogTerm == r.RaftLog.LastTerm() && m.Index >= r.RaftLog.LastIndex())
	//- If the responser has voted another one or is beyond the requester, it cannot vote.
	canVote := ((r.Vote == None && r.Lead == None) || (r.Vote == m.From)) && isBeyond
	//fmt.Printf("%d %d %v %v\n", m.From, r.id, canVote, isBeyond)
	if canVote {
		r.electionElapsed = 0
		r.Vote = m.From
	}
	r.sendVoteResponse(m.From, !canVote)
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	//fmt.Printf("%d get append from %d\n", r.id, m.From)
	//! Your Code Here (2A).
	logTerm, err := r.RaftLog.Term(m.Index)
	if err != nil || logTerm != m.LogTerm { //- mismatch term
		i := min(m.Index, r.RaftLog.LastIndex())
		for ; i > r.RaftLog.committed; i-- {
			term, err := r.RaftLog.Term(i)
			if err != nil || term != logTerm {
				break
			}
		}
		r.sendAppendResponse(m.From, true, i)
		return
	}
	//- find conflict index
	for i, entry := range m.Entries {
		term, err := r.RaftLog.Term(entry.Index)
		if err != nil || term != entry.Term {
			r.RaftLog.append(m.Entries[i:])
		}
	}
	toCommit := min(m.Commit, m.Index+uint64(len(m.Entries)))
	if toCommit > r.RaftLog.committed {
		r.RaftLog.committed = toCommit
	}
	r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
}

func (r *Raft) handleAppendResponse(m pb.Message) {
	pr := r.Prs[m.From]
	if m.Reject {
		//- non-leader node refused to append
		// leader could bring a follower's log
		// into consistency with its own.
		//fmt.Printf("%d %d\n", pr.Next, m.Index+1)
		if m.Index >= pr.Match {
			pr.Next = m.Index + 1
			r.sendAppend(m.From)
		}
		return
	}
	//- synchronized
	pr.Match = m.Index
	pr.Next = m.Index + 1
	if r.doCommit() {
		r.broadcast(func(id uint64) { r.sendAppend(id) })
	}
	if m.From == r.leadTransferee && m.Index == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	}
}

func (r *Raft) appendEntries(entries []*pb.Entry) {
	lastIdx := r.RaftLog.LastIndex()
	for i, entry := range entries {
		entry.Term = r.Term
		entry.Index = lastIdx + uint64(i) + 1
		if entry.EntryType == pb.EntryType_EntryConfChange {
			if r.PendingConfIndex > r.RaftLog.applied {
				entry.EntryType = pb.EntryType_EntryNormal
				entry.Data = nil
			} else {
				r.PendingConfIndex = entry.Index
			}
		}
	}
	r.RaftLog.append(entries)
	lastIdx = r.RaftLog.LastIndex()
	pr := r.Prs[r.id]
	if pr.Match < lastIdx {
		pr.Match = lastIdx
		pr.Next = lastIdx + 1
	}
}

func (r *Raft) handleProposal(m pb.Message) {
	r.appendEntries(m.Entries)
	r.doCommit()
	r.broadcast(func(id uint64) { r.sendAppend(id) })
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	//! Your Code Here (2A).
	if r.RaftLog.committed < m.Commit {
		r.RaftLog.committed = m.Commit
	}
	r.sendHeartbeatResponse(m.From, false)
}

func (r *Raft) handleHeartbeatResponse(m pb.Message) {
	if r.Prs[m.From].Match < r.RaftLog.LastIndex() {
		r.sendAppend(m.From)
	}
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	//! Your Code Here (2C).
	s := m.Snapshot
	sIdx, sTerm := s.Metadata.Index, s.Metadata.Term
	rTerm, err := r.RaftLog.Term(sIdx)
	//if sIdx <= r.RaftLog.committed {
	//	r.sendAppendResponse(m.From, true, r.RaftLog.committed)
	//	return
	//}
	if err == nil && rTerm == sTerm {
		r.sendAppendResponse(m.From, false, r.RaftLog.committed)
		return
	}

	r.RaftLog.restore(s)
	//- update ConfState
	newPrs := make(map[uint64]*Progress)
	lastIdx := r.RaftLog.LastIndex()
	for _, p := range s.Metadata.ConfState.Nodes {
		newPrs[p] = &Progress{Match: 0, Next: lastIdx + 1}
		if p == r.id {
			newPrs[p].Match = lastIdx
		}
	}
	r.Prs = newPrs
	r.sendAppendResponse(m.From, false, r.RaftLog.LastIndex())
}

func (r *Raft) handleTransferLeader(m pb.Message) {
	if r.State == StateFollower {
		//- transmit
		m.To = r.Lead
		r.send(m)
		return
	}
	if m.From == r.id {
		//- self-transfer
		return
	}
	if r.leadTransferee != None && m.From == r.leadTransferee {
		//- pending
		return
	}
	if !r.isValidID(m.From) {
		//- invalid id
		return
	}
	r.leadTransferee = m.From
	if r.Prs[m.From].Match == r.RaftLog.LastIndex() {
		r.sendTimeoutNow(m.From)
	} else {
		r.sendAppend(m.From)
	}
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	//! Your Code Here (3A).
	r.PendingConfIndex = None
	if !r.isValidID(id) {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	//! Your Code Here (3A).
	r.PendingConfIndex = None
	if !r.isValidID(id) {
		return
	}
	delete(r.Prs, id)
	if id != r.id {
		r.doCommit()
	}
}
