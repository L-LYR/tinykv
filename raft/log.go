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
	// These two variables stores the index and term of the previous snapshot
	// which will make the functions more simple and explicit.
	// They will be updated in maybeCompact.
	prevSnapIdx  uint64
	prevSnapTerm uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	//! Your Code Here (2A).
	if storage == nil {
		panic("storage mustn't be nil")
	}
	//- This cannot be called here, which may be nil.
	//- Do this at newRaft().
	// hardState, _, _ := storage.InitialState()
	var lo, hi, term uint64
	var err error
	if lo, err = storage.FirstIndex(); err != nil {
		panic(err)
	}
	if hi, err = storage.LastIndex(); err != nil {
		panic(err)
	}
	if term, err = storage.Term(lo - 1); err != nil {
		panic(err)
	}
	//- [lo,hi]
	var ents []pb.Entry
	if ents, err = storage.Entries(lo, hi+1); err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:         storage,
		committed:       lo - 1,
		applied:         lo - 1,
		stabled:         hi,
		entries:         ents,
		pendingSnapshot: nil,
		prevSnapIdx:     lo - 1,
		prevSnapTerm:    term,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	//! Your Code Here (2C).
	var err error
	var firstIdx, prevTerm uint64

	if firstIdx, err = l.storage.FirstIndex(); err != nil {
		panic(err)
	}
	if prevTerm, err = l.storage.Term(firstIdx - 1); err != nil {
		panic(err)
	}

	if firstIdx > l.FirstIndex() && len(l.entries) > 0 {
		if firstIdx > l.LastIndex() {
			l.entries = nil
		} else {
			l.entries = l.entries[l.toSliceIndex(firstIdx):]
		}
		l.prevSnapIdx = firstIdx - 1
		l.prevSnapTerm = prevTerm
	}
}

func (l *RaftLog) snapshot() (pb.Snapshot, error) {
	if l.pendingSnapshot != nil {
		return *l.pendingSnapshot, nil
	}
	return l.storage.Snapshot()
}

func (l *RaftLog) handleSnapshot(s *pb.Snapshot) {
	sIdx := s.Metadata.Index
	l.committed = sIdx
	l.entries = nil
	l.prevSnapIdx = sIdx
	l.prevSnapTerm = s.Metadata.Term
	l.pendingSnapshot = s
}

func (l *RaftLog) advanceSnapshot(s *pb.Snapshot) {
	sIdx := s.Metadata.Index
	l.stabled = max(l.stabled, sIdx)
	l.applied = max(l.applied, sIdx)
	if sIdx == l.pendingSnapshot.Metadata.Index {
		l.pendingSnapshot = nil
	}
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	//! Your Code Here (2A).
	beginIdx := l.toSliceIndex(l.stabled + 1)
	if int(beginIdx) > len(l.entries) {
		return nil
	}
	return l.entries[beginIdx:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() []pb.Entry {
	//! Your Code Here (2A).
	if l.committed > l.applied {
		beginIdx := l.toSliceIndex(l.applied + 1)
		endIdx := l.toSliceIndex(l.committed + 1)
		return l.entries[beginIdx:endIdx]
	}
	return nil
}

// Entries returns entries [lo, lastIdx].
// NOTE: inputs are indexes of entries
func (l *RaftLog) Entries(lo uint64) ([]pb.Entry, error) {
	if lo < l.FirstIndex() {
		return nil, ErrUnavailable
	}
	if lo > l.LastIndex() || l.LastIndex() == 0 {
		return nil, nil
	}
	return l.entries[l.toSliceIndex(lo):], nil
}

func (l *RaftLog) getEntries(lo uint64) ([]pb.Entry, error) {
	if lo < l.FirstIndex() {
		return nil, ErrUnavailable
	}
	lastIndex := l.LastIndex()
	if lo > lastIndex || lastIndex == 0 {
		return nil, nil
	}
	return l.entries[l.toSliceIndex(lo):], nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	//! Your Code Here (2A).
	if curLen := len(l.entries); curLen > 0 {
		return l.entries[curLen-1].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return l.prevSnapIdx
	//ret, _ := l.storage.LastIndex()
	//return ret
}

// FirstIndex return the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	if l.pendingSnapshot != nil {
		return l.pendingSnapshot.Metadata.Index
	}
	return l.prevSnapIdx
	//return 0
}

// LastTerm return the log entry with the last index
func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		panic(err)
	}
	return term
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	//! Your Code Here (2A).
	//length := uint64(len(l.entries))
	//idx := i - l.FirstIndex()
	//if length > 0 && idx >= 0 && idx < length { // in range
	//	return l.entries[l.toSliceIndex(i)].Term, nil
	//}
	//term, err := l.storage.Term(i)
	//if err != nil && !IsEmptySnap(l.pendingSnapshot) {
	//	if i == l.pendingSnapshot.Metadata.Index {
	//		return l.pendingSnapshot.Metadata.Term, nil
	//	}
	//	if i < l.pendingSnapshot.Metadata.Index {
	//		return 0, ErrCompacted
	//	}
	//}
	//return term, err
	// If i is equal to the previous snapshot index,
	// just return the previous snapshot term.
	if i == l.prevSnapIdx {
		return l.prevSnapTerm, nil
	}
	// i is less than the index of the first entry,
	// it will lead to an ErrCompacted.
	if i < l.FirstIndex() {
		return 0, ErrCompacted
	}
	// If current entries is empty, i is meaningless.
	// Entries have been compacted.
	if len(l.entries) == 0 {
		return 0, ErrCompacted
	}
	// If i is greater than the last index of the last entry,
	// obviously the entry is unavailable
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[l.toSliceIndex(i)].Term, nil
}

// append new entries behind the raftlog's entries
func (l *RaftLog) append(entps []*pb.Entry) {
	newFirstEntIdx := entps[0].Index
	if newFirstEntIdx != l.LastIndex()+1 {
		l.stabled = min(newFirstEntIdx-1, l.stabled)
		l.entries = append([]pb.Entry{}, l.entries[:l.toSliceIndex(newFirstEntIdx)]...)
	}
	for _, ent := range entps {
		l.entries = append(l.entries, *ent)
	}
	l.maybeCompact()
}

// toSliceIndex
func (l *RaftLog) toSliceIndex(entryIndex uint64) uint64 {
	return entryIndex - l.FirstIndex()
}

// toEntryIndex
func (l *RaftLog) toEntryIndex(sliceIndex uint64) uint64 {
	return sliceIndex + l.FirstIndex()
}
