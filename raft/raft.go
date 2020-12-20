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

// VoteType represents the current situation of vote stage.
type VoteType uint64

const (
	VoteWon VoteType = iota
	VoteLost
	VotePending
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
	for _, p := range c.peers {
		prs[p] = &Progress{}
	}
	//- init a new raft peer
	return &Raft{
		id:      c.ID,
		Term:    None,
		Vote:    None,
		RaftLog: newLog(c.Storage),
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
	m := pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, m)
	return true
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
	}
	r.msgs = append(r.msgs, m)
}

// sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
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

// resetTicks clear the electionElapsed and heartbeatElapsed
// and re-roll a new random election time. Used in doElection.
func (r *Raft) resetTicks() {
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.randElectionTime = r.electionTimeout + rand.Intn(r.electionTimeout)
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
	//- send the first heartbeat immediately
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgBeat,
		To:      r.id,
		Term:    r.Term,
	})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) (err error) {
	//! Your Code Here (2A).

	if m.Term > r.Term {
		//- test_paper_test.go:52:testUpdateTermFromMessage
		if m.MsgType == pb.MessageType_MsgHeartbeat {
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
		for pid := range r.Prs {
			if r.id != pid {
				r.sendAppend(pid)
			}
		}
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
		r.becomeFollower(m.Term, m.From)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.getVote(m.From, m.Reject)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return nil
}

// doElection begins a new election by broadcasting vote requests
func (r *Raft) doElection(m pb.Message) {
	r.resetTicks()
	r.becomeCandidate()
	//- only one peer
	if len(r.Prs) <= 1 {
		r.becomeLeader()
	}
	for p := range r.Prs {
		if p != r.id {
			r.sendVoteRequest(p)
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
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.sendHeartbeatResponse(m.From)
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
