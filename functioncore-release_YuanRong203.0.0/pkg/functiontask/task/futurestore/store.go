/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package futurestore implements the future store.
package futurestore

import (
	"sync"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/functiontask/task/config"
	"functioncore/pkg/functiontask/task/futurestore/snfuture"
)

const (
	defaultIDLength = 10
)

// FutureStore represents the future store.
type FutureStore struct {
	mutex       *sync.RWMutex
	futureStore map[snfuture.ID]*snfuture.Future
}

var stores []*FutureStore

// New creates a new future store.
func New() *FutureStore {
	store := &FutureStore{
		futureStore: make(map[snfuture.ID]*snfuture.Future),
		mutex:       new(sync.RWMutex),
	}
	stores = append(stores, store)
	return store
}

// Detail returns the detail of the future stores.
func Detail() (futures []map[string]interface{}) {
	for i, store := range stores {
		store.mutex.RLock()
		for _, f := range store.futureStore {
			futures = append(futures, map[string]interface{}{
				"store":     i,
				"FuncName":  f.FuncName,
				"StateID":   f.StateID,
				"ServiceID": f.ServiceID,
				"ReqIDList": f.ReqIDList,
				"ID":        f.ID,
			})
		}
		store.mutex.RUnlock()
	}
	return
}

// GetTTLFuture obtains the ID of the ttl future
func (s *FutureStore) GetTTLFuture() []string {
	s.mutex.RLock()
	futureIDs := make([]string, 0, defaultIDLength)
	for id, future := range s.futureStore {
		if future.SetTime.IsZero() {
			continue
		}
		existTime := time.Since(future.SetTime)
		if existTime > config.Conf.FutureTTL {
			futureIDs = append(futureIDs, id)
			log.GetLogger().Infof("the future %s will be deleted due to TTL: %fs, futureTTL: %fs",
				id, existTime.Seconds(), config.Conf.FutureTTL.Seconds())
		}
	}
	s.mutex.RUnlock()
	return futureIDs
}

// ClearFuturesByID clear future objects of all ID list
func (s *FutureStore) ClearFuturesByID(futureIDs []string) {
	s.mutex.Lock()
	for _, id := range futureIDs {
		delete(s.futureStore, id)
	}
	s.mutex.Unlock()
}

// Get returns the future by future id.
func (s *FutureStore) Get(id snfuture.ID) *snfuture.Future {
	s.mutex.RLock()
	f := s.futureStore[id]
	s.mutex.RUnlock()
	return f
}

// Add adds the future by future id.
func (s *FutureStore) Add(future *snfuture.Future) {
	s.mutex.Lock()
	s.futureStore[future.ID] = future
	s.mutex.Unlock()
	future.DeleteFromStore = s.deleteFromStore
}

func (s *FutureStore) deleteFromStore(future *snfuture.Future) {
	s.mutex.Lock()
	f := s.futureStore[future.ID]
	if f == future {
		delete(s.futureStore, future.ID)
	}
	s.mutex.Unlock()
}

// AddRefCount adds reference counting of the future by future id
func (s *FutureStore) AddRefCount(id string) int32 {
	s.mutex.RLock()
	f := s.futureStore[id]
	var cur int32
	// The logic of FunctionTask guarantees that the value of `f` here is not nil
	if f == nil {
		log.GetLogger().Errorf("adds reference count err, futureID: %s", id)
		cur = -1
	} else {
		cur = f.AddRefCount()
	}
	s.mutex.RUnlock()
	return cur
}

// SetIfNotExist sets the future by future id if it not exists.
func (s *FutureStore) SetIfNotExist(future *snfuture.Future) bool {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if _, exist := s.futureStore[future.ID]; exist {
		return false
	}
	s.futureStore[future.ID] = future
	future.DeleteFromStore = s.deleteFromStore
	return true
}

// Delete deletes the future by future id.
func (s *FutureStore) Delete(id string) {
	log.GetLogger().Debugf("inMemoryFutureStore deletes the future, id: %s", id)
	s.mutex.Lock()
	snf := s.futureStore[id]
	if snf == nil {
		log.GetLogger().Infof("deleteFuture, futureID: %s", id)
	} else if snf.ReduceRefCount() <= 0 {
		log.GetLogger().Debugf("inMemoryFutureStore deletes the future, id: %s", id)
		delete(s.futureStore, id)
	}
	s.mutex.Unlock()
}
