package nosql

import (
	"fmt"
	"github.com/dgryski/go-farm"
	"github.com/uber/cadence/common/collection"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
)

type (
	// shardedNosqlStore is a store that may have one or more shards
	shardingPolicy struct {
		logger        log.Logger
		metadataShard string
		policyConfig  *config.ShardingPolicy

		hasSharding        bool
		historyShardRanges []historyShardRange
	}

	historyShardRange struct {
		start     int
		end       int
		shardName string
	}
)

func NewShardingPolicy(logger log.Logger, metadataShard string, cfg *config.ShardingPolicy) (shardingPolicy, error) {
	sp := shardingPolicy{
		logger:        logger,
		metadataShard: metadataShard,
		policyConfig:  cfg,
	}

	err := sp.parse()
	if err != nil {
		return sp, err
	}

	return sp, nil
}

func (sp *shardingPolicy) parse() error {
	if sp.policyConfig == nil {
		return nil
	}
	sp.hasSharding = true

	// TODO: do sharding policy validation here
	sp.parseHistoryShardMapping()

	return nil
}

func (sp *shardingPolicy) parseHistoryShardMapping() {
	pq := collection.NewPriorityQueue(func(this interface{}, other interface{}) bool {
		return this.(historyShardRange).start < other.(historyShardRange).start
	})
	for shard, allocation := range sp.policyConfig.HistoryShardMapping {
		for _, r := range allocation.OwnedRanges {
			pq.Add(historyShardRange{
				start:     r.Start,
				end:       r.End,
				shardName: shard,
			})
		}
	}

	sp.historyShardRanges = []historyShardRange{}
	for !pq.IsEmpty() {
		sp.historyShardRanges = append(sp.historyShardRanges, pq.Remove().(historyShardRange))
	}
}

func (sp *shardingPolicy) getHistoryShardName(shardId int) string {
	if !sp.hasSharding {
		sp.logger.Info(fmt.Sprintf("Using the metadata shard (%v) for history shard %v", sp.metadataShard, shardId))
		return sp.metadataShard
	}

	for _, r := range sp.historyShardRanges {
		if shardId >= r.start && shardId <= r.end {
			sp.logger.Info(fmt.Sprintf("Using the %v shard for history shard %v", r.shardName, shardId))
			return r.shardName
		}
	}

	panic(fmt.Sprintf("Failed to identify store shard for shardId %v", shardId))
}

func (sp *shardingPolicy) getTaskListShardName(domainId string, taskListName string, taskType int) string {
	if !sp.hasSharding {
		sp.logger.Info(fmt.Sprintf("Using the metadata shard (%v) for tasklist %v", sp.metadataShard, taskListName))
		return sp.metadataShard
	}
	shards := sp.policyConfig.TaskListHashing.ShardOrder
	shardCount := len(shards)
	hash := farm.Hash32([]byte(domainId+"_"+taskListName)) % uint32(shardCount)
	shardIndex := int(hash) % shardCount

	sp.logger.Info(fmt.Sprintf("Using the %v shard for tasklist %v", shards[shardIndex], taskListName))
	return shards[shardIndex]
}
