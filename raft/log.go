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
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap/log"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	//- offset is the upperbound of the compacted logs(snapshot)
	//- the beginning index of entries
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	//! Your Code Here (2A).
	if storage == nil {
		log.Panic("storage mustn't be nil")
	}
	//- This cannot be called here, which may be nil.
	//- Do this at newRaft().
	// hardState, _, _ := storage.InitialState()
	lo, _ := storage.FirstIndex()
	hi, _ := storage.LastIndex()
	//- [lo,hi]
	ents, err := storage.Entries(lo, hi+1)
	if err != nil {
		log.Panic(err.Error())
	}
	return &RaftLog{
		storage:   storage,
		committed: None,
		applied:   None,
		offset:    lo,
		stabled:   hi,
		entries:   ents,
		// Used in 2C
		pendingSnapshot: &pb.Snapshot{},
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if curLen := len(l.entries); curLen > 0 {
		return l.entries[l.stabled-l.entries[0].Index+1 : curLen]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[l.applied-l.offset+1 : l.committed-l.offset+1]
	}
	return nil
}

func (l *RaftLog) Entries(lo uint64, hi uint64) []pb.Entry {
	return l.entries[lo-l.offset+1 : hi-l.offset]
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	//! Your Code Here (2A).
	if curLen := len(l.entries); curLen > 0 {
		return l.entries[curLen-1].Index
	}
	idx, _ := l.storage.LastIndex()
	return idx
}

// LastTerm return the log entry with the last index
func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panic(err.Error())
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	//! Your Code Here (2A).
	if i > l.LastIndex() { //- too big
		return 0, nil
	}
	if i >= l.offset { //- the target term in un-compacted entries
		if len(l.entries) > 0 {
			curIdx := i - l.offset
			return l.entries[curIdx].Term, nil
		} else { //- too big
			return 0, nil
		}
	}
	return l.storage.Term(i)
}
