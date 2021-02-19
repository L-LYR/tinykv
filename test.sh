#!/bin/bash
GOTEST="go test -v --count=1 --parallel=1 -p=1"

ServerTestLoc="./kv/server"
RaftTestLoc="./raft"
RaftStoreTestLoc="./kv/test_raftstore"

Project1TestArgs=("1")
Project2ATestArgs=("2A")
Project2BTestArgs=(
  "TestBasic2B"
  "TestConcurrent2B"
  "TestUnreliable2B"
  "TestOnePartition2B"
  "TestManyPartitionsOneClient2B"
  "TestManyPartitionsManyClients2B"
  "TestPersistOneClient2B"
  "TestPersistConcurrent2B"
  "TestPersistConcurrentUnreliable2B"
  "TestPersistPartition2B"
  "TestPersistPartitionUnreliable2B"
)
Project2CRaftTestArgs=("2C")
Project2CStoreTestArgs=(
  "TestOneSnapshot2C"
  "TestSnapshotRecover2C"
  "TestSnapshotRecoverManyClients2C"
  "TestSnapshotUnreliable2C"
  "TestSnapshotUnreliableRecover2C"
  "TestSnapshotUnreliableRecoverConcurrentPartition2C"
)
Project3ATestArgs=("3A")
Project3BTestArgs=(
  "TestTransferLeader3B"
  "TestBasicConfChange3B"
  "TestConfChangeRecover3B"
  "TestConfChangeRecoverManyClients3B"
  "TestConfChangeUnreliable3B"
  "TestConfChangeUnreliableRecover3B"
  "TestConfChangeSnapshotUnreliableRecover3B"
  "TestConfChangeSnapshotUnreliableRecoverConcurrentPartition3B"
  "TestOneSplit3B"
  "TestSplitRecover3B"
  "TestSplitRecoverManyClients3B"
  "TestSplitUnreliable3B"
  "TestSplitUnreliableRecover3B"
  "TestSplitConfChangeSnapshotUnreliableRecover3B"
  "TestSplitConfChangeSnapshotUnreliableRecoverConcurrentPartition3B"
)

i=0
FailedTest=()

doTest() {
  local TestArr=$1
  for arg in ${TestArr[*]}; do
    cmd="$GOTEST $2 -run $arg"
    printf "%s\n" "$cmd"
    if ! $cmd; then
      FailedTest[$i]=${TestArr[$i]}
      i=$((i + 1))
    fi
  done
}

if [ "$1" == "project1" ]; then
  doTest "${Project1TestArgs[*]}" $ServerTestLoc
elif [ "$1" == "project2a" ]; then
  doTest "${Project2ATestArgs[*]}" $RaftTestLoc
elif [ "$1" == "project2b" ]; then
  doTest "${Project2BTestArgs[*]}" $RaftStoreTestLoc
elif [ "$1" == "project2c" ]; then
  doTest "${Project2CRaftTestArgs[*]}" $RaftTestLoc
  doTest "${Project2CStoreTestArgs[*]}" $RaftStoreTestLoc
elif [ "$1" == "project3a" ]; then
  doTest "${Project3ATestArgs[*]}" $RaftTestLoc
elif [ "$1" == "project3b" ]; then
  doTest "${Project3BTestArgs[*]}" $RaftStoreTestLoc
fi

if [ ${#FailedTest[*]} -ne 0 ]; then
  printf "Fail in the following tests:\n"
  for name in ${FailedTest[*]}; do
    printf "%s\n" "$name"
  done
else
  printf "Pass all test of %s!\n" "$1"
fi
