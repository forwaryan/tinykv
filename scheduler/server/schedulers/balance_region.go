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

	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/filter"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

// init 注册 balance-region scheduler。
// scheduler server 后续可以根据配置创建这个 scheduler。
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
	filters      []filter.Filter
}

// newBalanceRegionScheduler 创建一个用于均衡各 store 上 Region 分布的 scheduler。
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	s.filters = []filter.Filter{
		filter.StoreStateFilter{ActionScope: s.GetName(), MoveRegion: true},
	}
	return s
}

// BalanceRegionCreateOption 表示创建 balanceRegionScheduler 时的可选配置。
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

// GetName 返回 scheduler 实例名，用于日志和 API 展示。
func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

// GetType 返回 scheduler 注册时使用的类型名。
func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

// IsScheduleAllowed 判断当前是否还能继续创建 Region 调度 operator。
func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// Schedule 选择 source store、target store 和要移动的 Region。
// Lab3C 会在这里实现真正的 Region 均衡决策。
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	// Your Code Here (3C).
	stores := cluster.GetStores()

	sources := filter.SelectSourceStores(stores, s.filters, cluster)
	targets := filter.SelectTargetStores(stores, s.filters, cluster)

	sort.Slice(sources, func(i, j int) bool {
		return sources[i].GetRegionSize() > sources[j].GetRegionSize()
	})
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].GetRegionSize() < targets[j].GetRegionSize()
	})

	for _, source := range sources {
		for i := 0; i < balanceRegionRetryLimit; i++ {
			region := s.selectRegionToMove(cluster, source)
			if region == nil {
				break
			}
			if len(region.GetVoters()) != cluster.GetMaxReplicas() {
				continue
			}
			if op := s.createMovePeerOperator(cluster, region, source, targets); op != nil {
				return op
			}
		}
	}

	return nil
}

func (s *balanceRegionScheduler) selectRegionToMove(cluster opt.Cluster, source *core.StoreInfo) *core.RegionInfo {
	sourceID := source.GetID()

	if region := cluster.RandPendingRegion(sourceID, core.HealthRegionAllowPending()); region != nil {
		return region
	}
	if region := cluster.RandFollowerRegion(sourceID, core.HealthRegion()); region != nil {
		return region
	}
	return cluster.RandLeaderRegion(sourceID, core.HealthRegion())
}

func (s *balanceRegionScheduler) createMovePeerOperator(cluster opt.Cluster, region *core.RegionInfo, source *core.StoreInfo, targets []*core.StoreInfo) *operator.Operator {
	sourceID := source.GetID()

	for _, target := range targets {
		targetID := target.GetID()
		if targetID == sourceID {
			continue
		}
		if region.GetStorePeer(targetID) != nil {
			continue
		}
		if source.GetRegionSize()-target.GetRegionSize() <= 2*region.GetApproximateSize() {
			return nil
		}

		newPeer, err := cluster.AllocPeer(targetID)
		if err != nil {
			continue
		}

		op, err := operator.CreateMovePeerOperator(
			s.GetName(),
			cluster,
			region,
			operator.OpBalance,
			sourceID,
			targetID,
			newPeer.GetId(),
		)
		if err != nil {
			continue
		}
		return op
	}

	return nil
}
