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

package frontend

import (
	"context"
	"strings"
	"time"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/frontend-invoke/constant"
)

// watchOption is the option for watcher
type watchOption struct {
	// Handler process the event
	Handler func(*etcd3.Event) error
	// Filter determine whether to process the event
	Filter        func(string) bool
	Path          string
	StopCh        <-chan struct{}
	SuccessCh     chan string
	ETCDSelection etcd3.EtcdType
	RecordRev     bool
}

const keepConnAliveTTL = 3 * time.Second

func processWatcher(opt *watchOption) {
	client := etcd3.GetETCDClient(opt.ETCDSelection)
	watchChan, rev := &etcd3.EtcdWatchChan{}, int64(0)
	for {
		if client.Client == nil {
			time.Sleep(keepConnAliveTTL)
			continue
		}
		watchChan = client.Watch(context.Background(), opt.Path, 0, true)
		if checkInitSucceed(opt, watchChan) {
			break
		}
		watchChan = client.Watch(context.Background(), opt.Path, 0, true)
	}

	errHandler := func(event *etcd3.Event) {
		log.GetLogger().Errorf("etcd event has error of %s: %s", opt.Path, string(event.Value))
		if strings.Contains(string(event.Value), "compacted") {
			log.GetLogger().Warnf("revision has been compacted, reset rev to 0")
			rev = 0
		}
	}

	for {
		select {
		case event, ok := <-watchChan.ResultChan:
			if !ok {
				log.GetLogger().Warnf("the channel received the result may be closed and try to reconnect from rev"+
					" %d of %s", rev, opt.Path)
				watchChan.Stop()
				watchChan = client.Watch(context.Background(), opt.Path, rev, true)
				time.Sleep(1 * time.Second)
				continue
			}
			if event.Type == etcd3.ERROR {
				errHandler(event)
			}
			if !opt.Filter(event.Key) {
				continue
			}
			if opt.RecordRev && event.Rev > rev {
				rev = event.Rev
			}
			if err := opt.Handler(event); err != nil {
				log.GetLogger().Errorf("error when process Function event %s: %s", event.Key, err.Error())
				continue
			}
		case <-opt.StopCh:
			log.GetLogger().Infof("received termination signal, shutdown processWatcher, path: %s", opt.Path)
			return
		}
	}
}

func checkInitSucceed(opt *watchOption, watchChan *etcd3.EtcdWatchChan) bool {
	if opt.Path != constant.WorkerInstancePrefix {
		return true
	}
	if isNormalConnection(watchChan) {
		close(opt.SuccessCh)
		return true
	}
	return false
}

func isNormalConnection(watchChan *etcd3.EtcdWatchChan) bool {
	return watchChan != nil
}
