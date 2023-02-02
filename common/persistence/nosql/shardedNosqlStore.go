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
	"sync"

	p "github.com/uber/cadence/common/persistence"

	"github.com/uber/cadence/common/config"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
)

type (
	// shardedNosqlStore is a store that may have one or more shards
	shardedNosqlStore struct {
		sync.RWMutex

		config config.ShardedNoSQL
		dc     *p.DynamicConfiguration
		logger log.Logger

		connectedShards map[string]nosqlStore
		defaultShard    nosqlStore
		shardingPolicy  shardingPolicy
	}
)

func newShardedNosqlStore(cfg config.ShardedNoSQL, logger log.Logger, dc *p.DynamicConfiguration) (*shardedNosqlStore, error) {
	//TODO: validate config
	// - has sharding policy
	// - has at least one connection
	// - metadatashard name is valid
	// - historyShardMapping is valid

	sn := shardedNosqlStore{
		config: cfg,
		dc:     dc,
		logger: logger,
	}

	// Connect to the default shard
	defaultShardName := cfg.DefaultShard
	store, err := sn.connectToShard(cfg.Connections[defaultShardName].NoSQLPlugin, defaultShardName)
	if err != nil {
		return nil, err
	}
	sn.defaultShard = *store
	sn.connectedShards = map[string]nosqlStore{
		defaultShardName: sn.defaultShard,
	}

	// Parse & validate the sharding policy
	sn.shardingPolicy, err = newShardingPolicy(logger, cfg)
	if err != nil {
		return nil, err
	}

	return &sn, nil
}

func (sn *shardedNosqlStore) GetStoreShardByHistoryShard(shardID int) (*nosqlStore, error) {
	shardName := sn.shardingPolicy.getHistoryShardName(shardID)
	return sn.getShard(shardName)
}

func (sn *shardedNosqlStore) GetStoreShardByTaskList(domainID string, taskListName string, taskType int) (*nosqlStore, error) {
	shardName := sn.shardingPolicy.getTaskListShardName(domainID, taskListName, taskType)
	return sn.getShard(shardName)
}

func (sn *shardedNosqlStore) GetDefaultShard() nosqlStore {
	// TODO: ensure default shard is set
	return sn.defaultShard
}

func (sn *shardedNosqlStore) Close() {
	sn.defaultShard.Close()

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
		return nil, &ShardingError{
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
	db, err := NewNoSQLDB(cfg, sn.logger, sn.dc)
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
