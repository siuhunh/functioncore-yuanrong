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

package network

import (
	"context"
	"fmt"
	"os/exec"
	"regexp"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snerror"
	"functioncore/pkg/worker/utils"
)

const (
	ipPattern = "((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})(\\.((2(5[0-5]|[0-4]\\d))|[0-1]?\\d{1,2})){3}"
)

// Pinger provides the ping method.
type Pinger struct {
	ip       string
	ip6      string
	interval int
	timeout  int
	checksum string
}

// NewPinger Create a ping processor
func NewPinger(IP, checksum string, ops ...Option) *Pinger {
	p := &Pinger{
		ip:       IP,
		ip6:      IP,
		interval: defaultInterval,
		timeout:  defaultTimeout,
		checksum: checksum,
	}
	for _, op := range ops {
		op(p)
	}
	return p
}

func (p *Pinger) checkPingHash() error {
	curHash := utils.GetHash(utils.PingPath)
	if len(p.checksum) != 0 && len(curHash) != 0 && p.checksum != curHash {
		return snerror.New(snerror.CheckSumErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.CheckSumErrorCode), "check ping sum error"))
	}
	return nil
}

func (p *Pinger) checkPing6Hash() error {
	curHash := utils.GetHash(utils.Ping6Path)
	if len(p.checksum) != 0 && len(curHash) != 0 && p.checksum != curHash {
		return snerror.New(snerror.CheckSumErrorCode,
			fmt.Sprintf(snerror.ErrText(snerror.CheckSumErrorCode), "check ping sum error"))
	}
	return nil
}

// PingRetry if failed to ping the destination IP address, an error message is returned.
func (p *Pinger) PingRetry(retryTimes int) error {
	matched, err := regexp.MatchString(ipPattern, p.ip)
	if !matched || err != nil {
		log.GetLogger().Warnf("failed to connect to %s", p.ip)
		return err
	}
	if err := p.checkPingHash(); err != nil {
		log.GetLogger().Errorf("failed to check ping sha 512")
		return err
	}
	for i := 0; i < retryTimes; i++ {
		command := exec.Command(
			"ping",
			p.ip,
			"-i",
			fmt.Sprintf("%d", p.interval),
			"-W",
			fmt.Sprintf("%d", p.timeout),
			"-D",
			"-c",
			"1",
		)
		err = command.Run()
		if err == nil {
			break
		}
		log.GetLogger().Warnf("failed to connect to %s for the %d time", p.ip, i+1)
		// last retry failure will not sleep
		if i < defaultHealthCheckRetry-1 {
			time.Sleep(defaultRetryDelay * time.Second)
		}
	}
	if err != nil {
		log.GetLogger().Warnf("failed to connect to %s", p.ip)
		return err
	}
	log.GetLogger().Debugf("ping successful")
	return nil

}

// Ping6Retry if failed to ping the destination IP address, an error message is returned.
func (p *Pinger) Ping6Retry(retryTimes int) error {
	var err error
	if err := p.checkPing6Hash(); err != nil {
		log.GetLogger().Errorf("failed to check ping6 sha 512")
		return err
	}
	for i := 0; i < retryTimes; i++ {
		command := exec.Command(
			"ping6",
			p.ip6,
			"-i",
			fmt.Sprintf("%d", p.interval),
			"-W",
			fmt.Sprintf("%d", p.timeout),
			"-D",
			"-c",
			"1",
		)
		err = command.Run()
		if err == nil {
			break
		}
		log.GetLogger().Warnf("failed to connect to ipv6 %s for the %d time", p.ip, i+1)
		time.Sleep(defaultRetryDelay * time.Second)
	}
	if err != nil {
		log.GetLogger().Errorf("failed to connect to ipv6 %s", p.ip)
		return err
	}
	log.GetLogger().Debugf("ping6 successful")
	return nil
}

// Callback KeepPing callback
type Callback func()

// DoHealthCheck Keep pinging the destination network. If the destination network cannot be pinged,
// execute the callback function. 5 seconds between
func (p *Pinger) DoHealthCheck(ctx context.Context) error {
	ticker := time.NewTicker(defaultRetryTime)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			err := p.PingRetry(defaultHealthCheckRetry)
			if err != nil {
				log.GetLogger().Errorf("failed to ping ip %s", p.ip)
				return err
			}
		case <-ctx.Done():
			log.GetLogger().Warnf("context is canceled when ping ip %s", p.ip)
			return nil
		}
	}
}
