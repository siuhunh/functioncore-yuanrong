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

package utils

import (
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/redisclient"
	"functioncore/pkg/common/utils"
	"functioncore/pkg/state-manager/config"
)

func InitRedisCmd(stopCh <-chan struct{}, conf *config.FConfig) error {
	var err error
	var pwd []byte
	pwd, err = localauth.Decrypt(conf.StateCounterCfg.Password)
	if err != nil {
		log.GetLogger().Errorf("failed to decrypt redis password, %s", err.Error())
		return err
	}
	redisCmd, err := redisclient.New(redisclient.NewRedisClientParam{
		ServerMode: conf.StateCounterCfg.ServerMode,
		ServerAddr: conf.StateCounterCfg.ServerAddr,
		Password:   string(pwd),
		Timeout:    conf.StateCounterCfg.TimeoutConf,
	}, stopCh, redisclient.SetEnableTLS(conf.StateCounterCfg.EnableTLS), redisclient.AlarmSwitchOn(),
		redisclient.SetGetRealTimeServerAddrFunc(config.GetHotloadMetricsConf))
	if err != nil {
		utils.ClearByteMemory(pwd)
		log.GetLogger().Errorf("failed to create a new redis client: %s", err.Error())
		return err
	}
	if err != nil {
		return err
	}
	redisclient.SetRedisCmd(redisCmd)
	return nil
}

func GetRedisCmd() *redisclient.Client {
	return redisclient.GetRedisCmd()
}
