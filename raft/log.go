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
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/offset.....applied....committed....stabled.....last
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
	// offset is the upperbound of the compacted logs(snapshot)
	// the beginning index of entries
	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	lo, err := storage.FirstIndex()
	if err != nil {
		log.Panic(err)
	}
	hi, err := storage.LastIndex()
	if err != nil {
		log.Panic(err)
	}
	ents, err := storage.Entries(lo, hi+1)
	if err != nil {
		log.Panic(err.Error())
	}
	rl := &RaftLog{
		storage:   storage,
		offset:    lo,
		applied:   lo - 1,
		committed: lo - 1,
		stabled:   hi,
		entries:   ents,
	}

	return rl
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
	stabled := int(l.stabled + 1 - l.offset)
	if stabled > len(l.entries) {
		return nil
	}
	return l.entries[stabled:]
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	beginIdx := max(l.applied+1, l.FirstIndex())
	if l.committed >= beginIdx {
		return l.entries[beginIdx-l.offset : l.committed+1-l.offset]
	}
	return nil
}

// FirstIndex returns the first index of the log entries
func (l *RaftLog) FirstIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[0].Index
	}
	idx, err := l.storage.FirstIndex()
	if err != nil {
		log.Panic(err)
	}
	return idx
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if curLen := len(l.entries); curLen > 0 {
		return l.entries[curLen-1].Index
	}
	idx, err := l.storage.LastIndex()
	if err != nil {
		log.Panic(err)
	}
	return idx
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) == 0 || i < l.offset {
		return l.storage.Term(i)
	}
	if i > l.LastIndex() {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.offset].Term, nil
}

// LastTerm return the term of the log entry with the last index
func (l *RaftLog) LastTerm() uint64 {
	term, err := l.Term(l.LastIndex())
	if err != nil {
		log.Panic(err)
	}
	return term
}

// EntrySliceFrom used in sendAppend, it will only return ErrCompacted,
// which is different from EntrySliceBetween.
func (l *RaftLog) EntrySliceFrom(i uint64) ([]pb.Entry, error) {
	if i < l.FirstIndex() {
		return nil, ErrCompacted
	}
	if i > l.LastIndex() || l.LastIndex() == 0 {
		return nil, nil
	}
	return l.entries[i-l.offset:], nil
}

// appendEntries will be called by r.appendEntries
func (l *RaftLog) appendEntries(entps []*pb.Entry) {
	// check whether the entries are committed.
	veryFirstIdx := entps[0].Index
	if veryFirstIdx <= l.committed {
		log.Panic("appending committed entries")
	}
	if veryFirstIdx != l.LastIndex()+1 {
		if veryFirstIdx <= l.stabled {
			log.Debugf("l.stabled move from %d to %d", l.stabled, veryFirstIdx-1)
			l.stabled = veryFirstIdx - 1
		}
		// drop the conflict ones, l.stabled may be moved forward
		l.entries = append([]pb.Entry{}, l.entries[:veryFirstIdx-l.offset]...)
	}
	// truncate entries
	for _, ent := range entps {
		l.entries = append(l.entries, *ent)
	}
	// maybeCompact
}
