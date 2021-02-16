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

// SnapStateType represents the state of snapshot.
// It will help to avoid repeatedly generate too many snapshots.
type SnapStateType uint64

const (
	StateNormal SnapStateType = iota
	StatePending
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

// ErrInvalidMsgType is returned when the m.MsgType is invalid.
var ErrInvalidMsgType = errors.New("invalid message type")

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

// Because in 2C, there will be a lot of snapshot request, SnapProgress is used to tell
// the leader whether and when to send or resend snapshot.
type SnapProgress struct {
	state      SnapStateType
	resendTick int
	pendingIdx uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress
	// snapshot progress of each peers
	sPrs map[uint64]*SnapProgress

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
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int
	// randomized election time which is in range [electiontimeout, 2 * electiontimeout - 1]
	// which will be changed in role switching.
	randomElectionTimeout int

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
	// Your Code Here (2A).
	l := newLog(c.Storage)
	r := &Raft{
		id:      c.ID,
		RaftLog: l,
		Prs:     make(map[uint64]*Progress),
		sPrs:    make(map[uint64]*SnapProgress),
		// when servers start up, they begin as followers.
		State:                 StateFollower,
		votes:                 make(map[uint64]bool),
		msgs:                  nil,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: c.ElectionTick + rand.Intn(c.ElectionTick),
	}

	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		log.Panic(err)
	}

	if !IsEmptyHardState(hs) {
		r.Term = hs.Term
		r.Vote = hs.Vote
		l.committed = hs.Commit
	}

	if c.Applied > 0 {
		l.applied = c.Applied
	}

	prs := c.peers
	if cs.Nodes != nil {
		prs = cs.Nodes
	}

	var prsList []string
	for _, id := range prs {
		r.sPrs[id] = &SnapProgress{}
		r.Prs[id] = &Progress{}
		prsList = append(prsList, fmt.Sprint(id))
	}
	log.Debugf("new Raft %d: peers[%s] RaftLog: offset[%d] applied[%d] committed[%d] stabled[%d]",
		r.id, strings.Join(prsList, ","), l.offset, l.applied, l.stabled, l.committed)
	return r
}

// This is my second attempt for TinyKV,
// so I try to rearrange the module structure.
// To not make this file too long,
// I divide functions into three parts:
// sending messages, handling messages,
// and election.
// election will be implemented in this file.
// sending messages and handling messages will be
// implemented in another two files,
// sendMsgs.go and handleMsgs.go

// lim returns the half number of peers, used for election.
func (r *Raft) lim() int { return len(r.Prs) / 2 }

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

// broadcast will call the same function fn on each peer
// used in updating progresses and broadcasting messages.
func (r *Raft) broadcast(fn func(id uint64), ignoreSelf bool) {
	for id := range r.Prs {
		if ignoreSelf && id == r.id {
			continue
		}
		fn(id)
	}
}

// tickHeartbeat can only be called by leader,
// who keeps heartbeatElapsed.
func (r *Raft) tickHeartbeat() {
	r.heartbeatElapsed++
	if r.heartbeatElapsed >= r.heartbeatTimeout {
		// if the leader receives a heartbeat tick,
		// it will send a MessageType_MsgHeartbeat
		// with m.Index = 0, m.LogTerm=0 and empty entries
		// as heartbeat to all followers.
		r.heartbeatElapsed = 0
		_ = r.Step(pb.Message{
			MsgType: pb.MessageType_MsgBeat,
			To:      r.id,
		})
	}
}

// tickElection will be called by followers.
// When occurring timeout, the follower will become
// candidate and raise a new election.
func (r *Raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomElectionTimeout {
		r.electionElapsed = 0
		_ = r.Step(pb.Message{
			MsgType: pb.MessageType_MsgHup,
			To:      r.id,
		})
	}
}

// tickResendSnap will be called by leader.
// When occurring timeout, the leader will set
// StatePending to StateNormal and then it can resend
// a snapshot.
func (r *Raft) tickResendSnap() {
	r.broadcast(func(id uint64) {
		spr := r.sPrs[id]
		if spr.state == StatePending {
			spr.resendTick++
			if spr.resendTick >= r.electionTimeout {
				spr.state = StateNormal
			}
		}
	}, true)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.tickHeartbeat()
		r.tickResendSnap()
	} else {
		r.tickElection()
	}
}

// resetStateInfo will be called when the r's role is changed.
// It will re-randomized a randomElectionTimeout, reset all the
// ticking-away variables, reset variables about election.
func (r *Raft) resetStateInfo() {
	// In most cases only a
	// single server(follower or candidate) will time out, which reduces the
	// likelihood of split vote in the new election.
	// re-rand when changing state
	// This can control the ratio of conflicts.
	r.randomElectionTimeout =
		r.electionTimeout + rand.Intn(r.electionTimeout)

	r.electionElapsed = 0
	r.heartbeatElapsed = 0

	r.votes = make(map[uint64]bool)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.resetStateInfo()
	r.State = StateFollower
	r.Lead = lead
	if term != r.Term {
		r.Term = term
		r.Vote = None
	}
	log.Debugf("%d become Follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.resetStateInfo()
	r.State = StateCandidate
	r.Lead = None
	r.Vote = r.id
	r.Term++
	log.Debugf("%d become Candidate at term %d", r.id, r.Term)
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	// Well, in the ./doc/project2-RaftKV.md, it said that
	// 'The tests assume that the newly elected leader should append a noop entry on its term'.
	//! I think 'append' is different with 'propose' in this project.
	r.resetStateInfo()
	r.State = StateLeader
	r.Lead = r.id
	lastIdx := r.RaftLog.LastIndex()
	r.broadcast(func(id uint64) {
		r.Prs[id].Match = None
		r.Prs[id].Next = lastIdx + 1
		r.sPrs[id].state = StateNormal
		r.sPrs[id].resendTick = 0
	}, false)
	r.Prs[r.id].Match = lastIdx
	log.Debugf("%d become Leader at term %d", r.id, r.Term)
	_ = r.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{{Data: nil}},
	})
	//r.appendEntries([]*pb.Entry{{Data: nil}})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	if m.Term == 0 {
		// local message
	} else if m.Term > r.Term {
		// If one server’s current term is
		// smaller than the other’s, then it updates its current term to the larger
		// value. If a candidate or leader discovers that its term is out of date,
		// it immediately reverts to follower state.
		// Also in stepCandidate.
		if m.MsgType == pb.MessageType_MsgHeartbeat ||
			m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgSnapshot {
			r.becomeFollower(m.Term, m.From)
		} else {
			r.becomeFollower(m.Term, None)
		}
	} else if m.Term < r.Term {
		//? ignore?
		log.Debugf("%d ignore a %s message from %d with term %d at term %d",
			r.id, m.MsgType, m.From, m.Term, r.Term)
		return nil
	}

	switch r.State {
	case StateFollower:
		return r.stepFollower(m)
	case StateCandidate:
		return r.stepCandidate(m)
	case StateLeader:
		return r.stepLeader(m)
	}
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// if a follower receives no communication
		// over election timeout, it begins an election to choose a new leader. It
		// increments its current term and transitions to candidate state. It then
		// votes for itself and issues RequestVote RPCs in parallel to each of the
		// other servers in the cluster.
		r.doElection()
	//case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	//case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	//case pb.MessageType_MsgRequestVoteResponse:
	case pb.MessageType_MsgSnapshot:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	//case pb.MessageType_MsgHeartbeatResponse:
	//case pb.MessageType_MsgTransferLeader:
	//case pb.MessageType_MsgTimeoutNow:
	default:
		log.Debugf("%d received an unexpected message from %d, type: %s, dropped", r.id, m.From, m.MsgType)
		//return ErrInvalidMsgType
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		// Also if a candidate fails to obtain a majority, it will time out and
		// start a new election by incrementing its term and initiating another
		// round of RequestVote RPCs.
		r.doElection()
	//case pb.MessageType_MsgBeat:
	case pb.MessageType_MsgPropose:
		return ErrProposalDropped
	case pb.MessageType_MsgAppend:
		// while waiting for votes,
		// if a candidate receives an AppendEntries RPC from another server claiming
		// to be leader whose term is at least as large as the candidate's current term,
		// it recognizes the leader as legitimate and returns to follower state.
		r.becomeFollower(m.Term, m.From)
		r.handleAppendEntries(m)
	//case pb.MessageType_MsgAppendResponse:
	case pb.MessageType_MsgRequestVote:
		// all state should handle MsgRequestVote RPC
		r.handleVoteRequest(m)
	case pb.MessageType_MsgRequestVoteResponse:
		r.receiveVote(m.From, m.Reject)
	case pb.MessageType_MsgSnapshot:
		r.becomeFollower(m.Term, m.From)
		r.handleSnapshot(m)
	case pb.MessageType_MsgHeartbeat:
		r.becomeFollower(m.Term, m.From)
		r.handleHeartbeat(m)
	//case pb.MessageType_MsgHeartbeatResponse:
	//case pb.MessageType_MsgTransferLeader:
	//case pb.MessageType_MsgTimeoutNow:
	default:
		log.Debugf("%d received an unexpected message from %d, type: %s, dropped", r.id, m.From, m.MsgType)
		//return ErrInvalidMsgType
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	switch m.MsgType {
	//case pb.MessageType_MsgHup:
	case pb.MessageType_MsgBeat:
		r.broadcast(r.sendHeartbeat, true)
	case pb.MessageType_MsgPropose:
		return r.handlePropose(m)
	//case pb.MessageType_MsgAppend:
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgRequestVote:
		r.handleVoteRequest(m)
	//case pb.MessageType_MsgRequestVoteResponse:
	//case pb.MessageType_MsgSnapshot:
	//case pb.MessageType_MsgHeartbeat:
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartbeatResponse(m)
	//case pb.MessageType_MsgTransferLeader:
	//case pb.MessageType_MsgTimeoutNow:
	default:
		log.Debugf("%d received an unexpected message from %d, type: %s, dropped", r.id, m.From, m.MsgType)
		//return ErrInvalidMsgType
	}
	return nil
}

// doElection is called by Followers when stepping MsgHup
func (r *Raft) doElection() {
	r.becomeCandidate()
	r.Vote = r.id
	r.receiveVote(r.id, false) // vote itself
	r.broadcast(r.sendVoteRequest, true)
}

// receiveVote is called by candidates after receiving vote responses.
func (r *Raft) receiveVote(from uint64, reject bool) {
	r.votes[from] = !reject
	gain, lose := 0, 0
	for _, pv := range r.votes {
		if pv {
			gain++
		} else {
			lose++
		}
	}
	// win the election when receiving votes from a majority of the servers
	if gain > r.lim() {
		r.becomeLeader()
		return
	}
	if lose > r.lim() {
		r.becomeFollower(r.Term, None)
		return
	}
	// stay in candidate if it does not obtain the majority
}
