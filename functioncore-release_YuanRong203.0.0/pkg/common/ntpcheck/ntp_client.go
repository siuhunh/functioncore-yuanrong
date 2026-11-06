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

// Package ntpcheck is used for NTP time inspection and correction and NTP alarm sending.
package ntpcheck

import (
	"fmt"
	"net"
	"os"
	"strings"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/tls"
)

const sslRoot = "SSL_ROOT"
const paasCrypto = "PAAS_CRYPTO_PATH"
const sshConnectLength = 4
const failedConnTimes = 15

var (
	hostName = ""
	localIP  = ""
)

// GetEnvValues is used geting HostName & local ip
func GetEnvValues() {
	hostName = os.Getenv("HOSTNAME")
	sshConnect := strings.Fields(os.Getenv("SSH_CONNECTION"))
	if len(sshConnect) == sshConnectLength {
		if address := net.ParseIP(sshConnect[2]); address != nil {
			localIP = address.String()
		}
	}
	log.GetLogger().Infof("hostName: %s, localIP: %s ", hostName, localIP)
}

// SetEnvValues is used setting the SSL_ROOT and PAAS_CRYPTO_PATH Environment Variables
func SetEnvValues(ntpTLSCfg ntpCheckTLSConfig) error {
	err := os.Setenv(sslRoot, ntpTLSCfg.SslPath)
	if err != nil {
		return err
	}
	err = os.Setenv(paasCrypto, ntpTLSCfg.CipherPath)
	if err != nil {
		return err
	}
	return nil
}

// SendAlarmToOmsvc is used sending alarms to OMSVC
func SendAlarmToOmsvc(content string) {
	log.GetLogger().Infof("Send alarm to omsvc.")
	// The message carries the global variable "hostName" to identify the identity.
	alarm.GetClient().ReportAlarm(alarm.NTPOffset, content)
}

// SendRemoveAlarmToOmsvc is used sending a clear alarm to the OMSVC.
func SendRemoveAlarmToOmsvc() {
	log.GetLogger().Infof("Send remove alarm to omsvc.")
	// The message carries the global variable "hostName" to identify the identity.
	alarm.GetClient().ClearAlarm(alarm.NTPOffset)
}

// InitAlarmClient is used alarm HTTPS client initialization
func InitAlarmClient() error {
	ntpTLSCfg := ntpCfg.TLSConfig
	GetEnvValues()
	err := SetEnvValues(ntpTLSCfg)
	if err != nil {
		log.GetLogger().Errorf("set env error: %s", err.Error())
		return err
	}

	stopCh := make(chan struct{}, 1)
	err = tls.InitTLSConfig(ntpTLSCfg.TLSProtocol, ntpTLSCfg.TLSCiphers)
	if err != nil {
		return err
	}

	if err = alarm.NewClient(ntpCfg.OmsvcAddress, "meta-etcd", stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth(ntpTLSCfg.AKey, ntpTLSCfg.SKey),
		alarm.WithSpecLocation(fmt.Sprintf("HostIP=%s;HostName=%s;", localIP, hostName))); err != nil {
		return err
	}

	alarm.GetClient().RegisterAlarmType(alarm.NTPOffset)
	alarm.GetClient().RegisterAlarmType(alarm.NTPConnection)
	return nil
}

// ReportNtpConnAlarm Report conn Alarm
func ReportNtpConnAlarm(failedConCnt int, content string) int {
	failedConCnt++
	if failedConCnt >= failedConnTimes && failedConCnt%failedConnTimes == 0 {
		log.GetLogger().Warnf("Report NTP connection alarm to omsvc.")
		alarm.GetClient().ReportAlarm(alarm.NTPConnection, content)
		return failedConCnt
	}
	return failedConCnt
}

// RemoveNtpConnAlarm Remove conn Alarm
func RemoveNtpConnAlarm(failedConCnt int) int {
	alarm.GetClient().ClearAlarm(alarm.NTPConnection)
	failedConCnt = 0
	log.GetLogger().Infof("Remove NTP connection alarm to omsvc.")
	return failedConCnt
}
