/*
 * Copyright (c) 2022 Huawei Technologies Co., Ltd
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

// Package tokenrotation is used to implement token rotation of cloud services.
package tokenrotation

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

const (
	alarmThresholdTimes = 3
	// retryTimes retry until next cron job
	retryTimes    = 71
	retryInterval = 10 * time.Minute
)

var (
	config   Config
	regionID = os.Getenv("REGION_ID")

	instance = &tokenRotation{
		mu: sync.RWMutex{},
	}
)

// Config for token rotation
type Config struct {
	ProjectID        string `json:"projectID" valid:"required"`
	IdentityProvider string `json:"identityProvider" valid:"required"`
	IAMHostName      string `json:"iamHostName" valid:"required"`
	CronSpec         string `json:"tokenCronSpec" valid:"required"`

	EnableTokenByAgency bool   `json:"enableTokenByAgency"`
	TokenAgencyName     string `json:"tokenAgencyName"`
	TokenAgencyDomain   string `json:"tokenAgencyDomain"`
}

type tokenRotation struct {
	mu            sync.RWMutex
	access        string
	secret        string
	securityToken string
}

// StartTokenRotation -
func StartTokenRotation(stopCh <-chan struct{}) error {
	cfgJSON := os.Getenv("TOKEN_ROTATION_CONFIG")
	if cfgJSON == "" || regionID == "" {
		log.GetLogger().Infof("disable token rotation")
		return nil
	}

	// parse config
	tmpCfg := &Config{}
	err := json.Unmarshal([]byte(cfgJSON), tmpCfg)
	if err != nil {
		return err
	}
	config = *tmpCfg

	return instance.start(stopCh)
}

// GetTempToken return unencrypted ak and encrypted sk/token
func GetTempToken() (string, string, string) {
	return instance.getTempToken()
}

func (r *tokenRotation) getTempToken() (string, string, string) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.access, r.secret, r.securityToken
}

func (r *tokenRotation) storeTempToken(token TempToken) error {
	defer func() {
		utils.ClearStringMemory(token.Secret)
		utils.ClearStringMemory(token.SecurityToken)
	}()
	sk, err := localauth.Encrypt(token.Secret)
	if err != nil {
		return err
	}
	stsToken, err := localauth.Encrypt(token.SecurityToken)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.access = token.Access
	r.secret = sk
	r.securityToken = stsToken
	r.mu.Unlock()
	return nil
}

func (r *tokenRotation) start(stopCh <-chan struct{}) error {
	tempToken, err := getTokenFromIAM()
	if err != nil {
		log.GetLogger().Errorf("failed to get token from IAM: %s", err.Error())
		return err
	}

	if err = r.storeTempToken(tempToken); err != nil {
		log.GetLogger().Errorf("failed to store temp token: %s", err.Error())
		return err
	}

	log.GetLogger().Infof("succeed to get and store temp token, enable agency token: %t", config.EnableTokenByAgency)
	if err = r.cronGetTokenFromIAM(stopCh); err != nil {
		log.GetLogger().Errorf("failed to start cron job: %s", err.Error())
		return err
	}
	alarm.GetClient().RegisterAlarmType(alarm.RotatedToken)
	return nil
}

func (r *tokenRotation) getTokenFromIAMWithRetry() {
	log.GetLogger().Infof("start to get new temp token: %v", time.Now())
	var err error
	for i := 0; i < retryTimes; i++ {
		var tempToken TempToken
		tempToken, err = getTokenFromIAM()
		if err != nil {
			log.GetLogger().Errorf("failed to get token from IAM: %s", err.Error())
		} else {
			if err = r.storeTempToken(tempToken); err != nil {
				log.GetLogger().Errorf("failed to store temp token: %s", err.Error())
			}
		}
		if err == nil {
			log.GetLogger().Infof("succeed to update temp token")
			alarm.GetClient().ClearAlarm(alarm.RotatedToken)
			break
		}
		if i == alarmThresholdTimes {
			alarm.GetClient().ReportAlarm(alarm.RotatedToken, fmt.Sprintf("error msg=%s,", err.Error()))
		}
		if i != retryTimes-1 {
			time.Sleep(retryInterval)
		}
	}
}

func (r *tokenRotation) cronGetTokenFromIAM(stopCh <-chan struct{}) error {
	if stopCh == nil {
		return errors.New("stopCh is nil")
	}
	c := cron.New(cron.WithSeconds())
	_, err := c.AddFunc(config.CronSpec, r.getTokenFromIAMWithRetry)
	if err != nil {
		return err
	}
	c.Start()
	go func() {
		select {
		case _ = <-stopCh:
		}
		c.Stop()
	}()
	return nil
}
