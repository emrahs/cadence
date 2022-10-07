package nosql

import (
	"fmt"
	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/types"
	"sync"
)

type (
	// shardedNosqlStore is a store that may have one or more shards
	shardedNosqlStore struct {
		sync.RWMutex

		logger log.Logger
		config config.ShardedNoSQL

		isSharded       bool
		connectedShards map[string]nosqlStore
		metadataShard   nosqlStore
		shardingPolicy  shardingPolicy
	}

	// UnknownShardError represents invalid shard
	UnknownShardError struct {
		Message string
	}
)

func NewShardedNosqlStore(logger log.Logger, cfg config.ShardedNoSQL) (*shardedNosqlStore, error) {
	// TODO: validate config
	// - has sharding policy
	// - has at least one connection
	// - metadatashard name is valid
	// - historyShardMapping is valid

	sn := shardedNosqlStore{
		logger: logger,
		config: cfg,
	}

	if cfg.ShardingPolicy != nil {
		sn.isSharded = true
		sp, err := NewShardingPolicy(logger, cfg.MetadataShard, cfg.ShardingPolicy)
		if err != nil {
			return nil, err
		}
		sn.shardingPolicy = sp
	}

	// set isSharded
	// set metadata shard
	var mdShardName string
	if cfg.ShardingPolicy == nil {
		mdShardName = config.NonShardedStoreName
	} else {
		mdShardName = cfg.MetadataShard
		sn.isSharded = true
	}

	s, err := sn.connectToShard(cfg.Connections[mdShardName].NoSQLPlugin, mdShardName)
	if err != nil {
		return nil, err
	}

	sn.metadataShard = *s
	sn.connectedShards = map[string]nosqlStore{
		mdShardName: sn.metadataShard,
	}

	return &sn, nil
}

func (sn *shardedNosqlStore) GetStoreShardByHistoryShard(shardId int) (*nosqlStore, error) {
	shardName := sn.shardingPolicy.getHistoryShardName(shardId)
	return sn.getShard(shardName)
}

func (sn *shardedNosqlStore) GetStoreShardByTaskList(domainId string, taskListName string, taskType int) (*nosqlStore, error) {
	shardName := sn.shardingPolicy.getTaskListShardName(domainId, taskListName, taskType)
	return sn.getShard(shardName)
}

func (sn *shardedNosqlStore) GetMetadataShard() nosqlStore {
	// TODO: ensure metadata shard is set
	return sn.metadataShard
}

func (sn *shardedNosqlStore) Close() {
	sn.metadataShard.Close()

	for name, shard := range sn.connectedShards {
		sn.logger.Warn("Closing store shard", tag.StoreShard(name))
		shard.Close()
	}
}

func (sn *shardedNosqlStore) GetName() string {
	return "shardedNosql"
}

func (sn *shardedNosqlStore) getShard(shardName string) (*nosqlStore, error) {
	sn.RLock()
	shard, found := sn.connectedShards[shardName]
	sn.RUnlock()

	if found {
		return &shard, nil
	}

	cfg, ok := sn.config.Connections[shardName]
	if !ok {
		return nil, &types.InternalServiceError{
			Message: fmt.Sprintf("Unknown db shard name: %v", shardName),
		}
	}

	sn.Lock()
	if shard, ok := sn.connectedShards[shardName]; ok { // read again to double-check
		sn.Unlock()
		return &shard, nil
	}

	s, err := sn.connectToShard(cfg.NoSQLPlugin, shardName)
	if err != nil {
		return nil, err
	}
	sn.connectedShards[shardName] = *s
	sn.logger.Info("Connected to store shard", tag.StoreShard(shardName))
	sn.Unlock()
	return s, nil
}

func (sn *shardedNosqlStore) connectToShard(cfg *config.NoSQL, shardName string) (*nosqlStore, error) {
	sn.logger.Info("Connecting to store shard", tag.StoreShard(shardName))
	db, err := NewNoSQLDB(cfg, sn.logger)
	if err != nil {
		sn.logger.Error("Failed to connect to store shard", tag.StoreShard(shardName), tag.Error(err))
		return nil, err
	}
	shard := nosqlStore{
		db:     db,
		logger: sn.logger,
	}
	return &shard, nil
}
