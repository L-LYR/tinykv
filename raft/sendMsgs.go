package raft

import (
	"fmt"

	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// msgShowList is a filter, it is used to show the sending info
// about the message whose type is *not* in.
var msgShowList = map[pb.MessageType]bool{
	pb.MessageType_MsgHup:                 true,
	pb.MessageType_MsgBeat:                true,
	pb.MessageType_MsgAppend:              true,
	pb.MessageType_MsgAppendResponse:      true,
	pb.MessageType_MsgRequestVote:         true,
	pb.MessageType_MsgRequestVoteResponse: true,
	pb.MessageType_MsgSnapshot:            true,
	pb.MessageType_MsgHeartbeat:           true,
	pb.MessageType_MsgHeartbeatResponse:   true,
	pb.MessageType_MsgTransferLeader:      true,
	pb.MessageType_MsgTimeoutNow:          true,
}

// send is a monitor function, which will show the sending info, depending on msgShowList.
func (r *Raft) send(m pb.Message) {
	if _, ok := msgShowList[m.MsgType]; !ok {
		info := fmt.Sprintf("%d send %v to %d at term %d with Index %d",
			m.From, m.MsgType, m.To, m.Term, m.Index)
		if m.MsgType == pb.MessageType_MsgRequestVoteResponse {
			info += fmt.Sprintf(", reject: %v", m.Reject)
		}
		if m.MsgType == pb.MessageType_MsgSnapshot {
			sm := m.Snapshot.Metadata
			info += fmt.Sprintf(", snapshot index: %d term: %d", sm.Index, sm.Term)
		}
		log.Debugf("%s", info)
	}
	r.msgs = append(r.msgs, m)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	nextIdx := r.Prs[to].Next
	log.Debugf("nextIdx = %d", nextIdx)
	term, getTermErr := r.RaftLog.Term(nextIdx - 1)
	ents, getEntryErr := r.RaftLog.EntrySliceFrom(nextIdx)
	if getTermErr != nil || getEntryErr != nil {
		log.Debugf("getTermErr: %v\ngetEntryErr: %v", getTermErr, getEntryErr)
		return r.sendSnapshot(to)
	}
	entps := make([]*pb.Entry, 0, len(ents))
	for i := range ents {
		entps = append(entps, &ents[i])
	}
	log.Debugf("%d send %d entries to %d", r.id, len(entps), to)
	m := pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Index:   nextIdx - 1,
		LogTerm: term,
		Entries: entps,
		Commit:  r.RaftLog.committed,
	}
	r.send(m)
	return false
}

// sendSnapshot sends snapshot to the given peer.
func (r *Raft) sendSnapshot(to uint64) bool {
	spr := r.sPrs[to]
	if spr.state == StatePending {
		return false
	}
	snapshot, err := r.RaftLog.snapshot()
	if err != nil {
		if err == ErrSnapshotTemporarilyUnavailable {
			log.Debugf("%d failed to send snapshot to %d", r.id, to)
			return false
		}
		log.Panic(err)
	}
	if IsEmptySnap(&snapshot) {
		log.Panicf("Empty Snapshot!")
	}
	m := pb.Message{
		MsgType:  pb.MessageType_MsgSnapshot,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		Snapshot: &snapshot,
	}
	r.send(m)
	spr.state = StatePending
	spr.resendTick = 0
	spr.pendingIdx = snapshot.Metadata.Index
	return true
}

// sendAppendResponse sends an append entries response to the given peer
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

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	// heartbeat message must not make followers synchronize with the leader.
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed),
	}
	r.send(m)
}

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

// sendHeartbeatResponse sends a heartbeat response to the given peer.
func (r *Raft) sendHeartbeatResponse(to uint64) {
	m := pb.Message{
		MsgType: pb.MessageType_MsgHeartbeatResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.send(m)
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
