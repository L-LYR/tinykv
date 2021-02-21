package message

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	rspb "github.com/pingcap-incubator/tinykv/proto/pkg/raft_serverpb"
)

type ApplyResultType int

const (
	ResType_CompactLog ApplyResultType = iota
	ResType_ChangePeer
	ResType_SplitRegion
)

// ApplyCmd is equal to proposal
// just a record to save callbacks
type ApplyCmd struct {
	Index uint64
	Term  uint64
	Cb    *Callback
}

// TODO: divide apply results into different structs
// The same with message, just one kind struct
type ApplyResult struct {
	ResType ApplyResultType
	// used for compact log
	CompactLogIndex uint64
	// used for change peer
	Peer       *metapb.Peer
	ConfChange *pb.ConfChange
	// used for split region
	OldRegion *metapb.Region
	// used for both split region and change peer
	NewRegion *metapb.Region
}

// apply msg
// TODO: divide apply msgs into different types
// Because the handler is already too complex,
// I will not split MsgApply into
// various types for the sake of simplicity.
type MsgApply struct {
	Id      uint64
	Term    uint64
	Region  *metapb.Region
	Update  bool // update is used to tell applier to update its region info
	Cmds    []*ApplyCmd
	Entries []pb.Entry
}

// apply result
type MsgApplyResult struct {
	ApplyState *rspb.RaftApplyState
	// corresponding to the peer.SizeDiffHint
	// use type int64 instead of uint64
	// for fear of underflow
	SizeDiffHint int64
	Results      []ApplyResult
}
