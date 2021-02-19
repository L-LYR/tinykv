// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) findSuitableStores(maxDownTime time.Duration, stores []*core.StoreInfo) []*core.StoreInfo {
	var suitable []*core.StoreInfo
	for _, store := range stores {
		if store.IsUp() && store.DownTime() < maxDownTime && store.IsAvailable() {
			suitable = append(suitable, store)
		}
	}
	// descending order
	sort.Slice(suitable, func(i, j int) bool {
		return suitable[i].GetRegionSize() > suitable[j].GetRegionSize()
	})
	return suitable
}

type GetSuitableRegion func(uint64, func(core.RegionsContainer))

func (s *balanceRegionScheduler) findMostSuitableRegion(cluster opt.Cluster, suitable []*core.StoreInfo) (*core.RegionInfo, *core.StoreInfo) {
	// get Functions
	// sorted from highest to lowest priority
	// Pending > Follower > Leader
	var get = []GetSuitableRegion{
		cluster.GetPendingRegionsWithLock,
		cluster.GetFollowersWithLock,
		cluster.GetLeadersWithLock,
	}
	var mostSuitableRegion *core.RegionInfo
	healthChecker := core.HealthRegionAllowPending()
	callback := func(container core.RegionsContainer) {
		mostSuitableRegion = container.RandomRegion(nil, nil)
		if mostSuitableRegion != nil && !healthChecker(mostSuitableRegion) {
			mostSuitableRegion = nil
		}
	}
	for _, store := range suitable {
		for _, getMostSuitableRegionFrom := range get {
			getMostSuitableRegionFrom(store.GetID(), callback)
			if mostSuitableRegion != nil {
				break
			}
		}
		// MaxReplicas is not mentioned in project's doc.
		// But it appears in test functions.
		// So here we need to check whether the region has reached the
		// MaxReplicas or not.
		if mostSuitableRegion != nil {
			if len(mostSuitableRegion.GetPeers()) < cluster.GetMaxReplicas() {
				mostSuitableRegion = nil
			} else {
				return mostSuitableRegion, store
			}
		}
	}
	return nil, nil
}

func (s *balanceRegionScheduler) findTargetStore(mostSuitableRegion *core.RegionInfo,
	srcStore *core.StoreInfo, suitable []*core.StoreInfo) *core.StoreInfo {
	// Find target store in region
	alternatives := mostSuitableRegion.GetStoreIds()
	// Search in ascending order
	for i := len(suitable) - 1; i >= 0; i-- {
		curStore := suitable[i]
		// Meet the source store. No target store any more.
		if curStore.GetID() == srcStore.GetID() {
			break
		}
		// The target store should be in a different region from the source store.
		if _, ok := alternatives[curStore.GetID()]; !ok {
			return curStore
		}
	}
	return nil
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	// find suitable stores
	suitable := s.findSuitableStores(cluster.GetMaxStoreDownTime(), cluster.GetStores())
	// there should be at least two stores to make movements.
	if len(suitable) <= 1 {
		return nil
	}
	// find most suitable region
	mostSuitableRegion, srcStore := s.findMostSuitableRegion(cluster, suitable)
	if mostSuitableRegion == nil {
		return nil
	}
	// find target store
	dstStore := s.findTargetStore(mostSuitableRegion, srcStore, suitable)
	if dstStore == nil {
		return nil
	}
	// Difference has to be bigger than two times the approximate size of the region
	// Ensure that after moving, the target store’s region size
	// is still smaller than the original store.
	sizeDiff := srcStore.GetRegionSize() - dstStore.GetRegionSize()
	if sizeDiff < 2*mostSuitableRegion.GetApproximateSize() {
		return nil
	}
	newPeer, err := cluster.AllocPeer(dstStore.GetID())
	if err != nil {
		log.Info(err)
		return nil
	}
	operators, err := operator.CreateMovePeerOperator("", cluster, mostSuitableRegion,
		operator.OpBalance, srcStore.GetID(), dstStore.GetID(), newPeer.GetId())
	if err != nil {
		log.Info(err)
		return nil
	}
	return operators
}
