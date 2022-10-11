// Copyright (c) 2022 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package nosql

import (
	"fmt"

	"github.com/dgryski/go-farm"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
)

type (
	// shardedNosqlStore is a store that may have one or more shards
	shardingPolicy struct {
		logger       log.Logger
		defaultShard string
		config       config.ShardedNoSQL

		hasShardedHistory  bool
		hasShardedTasklist bool
	}
)

func NewShardingPolicy(logger log.Logger, cfg config.ShardedNoSQL) (shardingPolicy, error) {
	sp := shardingPolicy{
		logger:       logger,
		defaultShard: cfg.DefaultShard,
		config:       cfg,
	}

	err := sp.parse()
	if err != nil {
		return sp, err
	}

	return sp, nil
}

func (sp *shardingPolicy) parse() error {
	sp.parseHistoryShardMapping()
	sp.parseTaskListShardingPolicy()
	return nil
}

func (sp *shardingPolicy) parseHistoryShardMapping() {
	historyShardMapping := sp.config.ShardingPolicy.HistoryShardMapping
	if len(historyShardMapping) == 0 {
		return
	}
	sp.hasShardedHistory = true
}

func (sp *shardingPolicy) parseTaskListShardingPolicy() {
	tlShards := sp.config.ShardingPolicy.TaskListHashing.ShardOrder
	if len(tlShards) == 0 {
		return
	}
	sp.hasShardedTasklist = true
}

func (sp *shardingPolicy) getHistoryShardName(shardId int) string {
	if !sp.hasShardedHistory {
		sp.logger.Info(fmt.Sprintf("Using the default shard (%v) for history shard %v", sp.defaultShard, shardId))
		return sp.defaultShard
	}

	for _, r := range sp.config.ShardingPolicy.HistoryShardMapping {
		if shardId >= r.Start && shardId <= r.End {
			sp.logger.Info(fmt.Sprintf("Using the %v shard for history shard %v", r.Shard, shardId))
			return r.Shard
		}
	}

	panic(fmt.Sprintf("Failed to identify store shard for shardId %v", shardId))
}

func (sp *shardingPolicy) getTaskListShardName(domainId string, taskListName string, taskType int) string {
	if !sp.hasShardedTasklist {
		sp.logger.Info(fmt.Sprintf("Using the default shard (%v) for tasklist %v", sp.defaultShard, taskListName))
		return sp.defaultShard
	}
	tlShards := sp.config.ShardingPolicy.TaskListHashing.ShardOrder
	tlShardCount := len(tlShards)
	hash := farm.Hash32([]byte(domainId+"_"+taskListName)) % uint32(tlShardCount)
	shardIndex := int(hash) % tlShardCount

	sp.logger.Info(fmt.Sprintf("Using the %v shard for tasklist %v", tlShards[shardIndex], taskListName))
	return tlShards[shardIndex]
}
