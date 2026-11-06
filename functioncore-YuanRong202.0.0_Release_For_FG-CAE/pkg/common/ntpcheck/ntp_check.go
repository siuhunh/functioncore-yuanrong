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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"os/exec"
	"time"

	"github.com/asaskevich/govalidator"

	"functioncore/pkg/common/logger/log"
)

const (
	epochOffsetFmt    = 2208988800
	timeLayoutFmt     = "2006-01-02 15:04:05"
	ntpServerTimeout  = 5 * time.Second
	maxNumOfNtpServer = 10
	offsetAlarm       = "The NTP offset is greater than 150 seconds."
	serverAlarm       = "The NTP Server is unavailable."
)

type packet struct {
	Settings       uint8
	Stratum        uint8
	Poll           int8
	Precision      int8
	RootDelay      uint32
	RootDispersion uint32
	ReferenceID    uint32
	RefTimeSec     uint32
	RefTimeFrac    uint32
	OrigTimeSec    uint32
	OrigTimeFrac   uint32
	RxTimeSec      uint32
	RxTimeFrac     uint32
	TxTimeSec      uint32
	TxTimeFrac     uint32
}

type ntpCheckTLSConfig struct {
	TLSProtocol string `json:"tls_protocol" valid:"required"`
	TLSCiphers  string `json:"tls_ciphers"  valid:"required"`
	AKey        string `json:"AKey"         valid:"required"`
	SKey        string `json:"SKey"         valid:"required"`
	SslPath     string `json:"ssl_path"     valid:"required"`
	CipherPath  string `json:"cipher_path"  valid:"required"`
}

type checkParam struct {
	Interval  int `json:"interval"   valid:"range(5|300)"`
	AlarmCnt  int `json:"alarm_cnt"  valid:"range(1|30)"`
	MaxOffset int `json:"max_offset" valid:"range(0|200)"`
}

// Config -
type Config struct {
	OmsvcAddress     string            `json:"omsvcAddress"  valid:"required"`
	NtpServeraddress map[string]string `json:"ntp_server"    valid:"required"`
	CheckParam       checkParam        `json:"check"         valid:"required"`
	TLSConfig        ntpCheckTLSConfig `json:"tls"           valid:"required"`
}

var (
	ntpCfg    *Config
	alarmFlag = false
	ntpStopCh = make(chan struct{})
)

// InitConfig -
func InitConfig(filePath string) error {
	config, err := loadConfig(filePath)
	if err != nil {
		return err
	}
	ntpCfg = config
	log.GetLogger().Infof("ntp config: %v", config)
	return nil
}

func loadConfig(configPath string) (*Config, error) {
	configData, err := ioutil.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	config := &Config{}
	err = json.Unmarshal(configData, config)
	if err != nil {
		return nil, err
	}

	_, err = govalidator.ValidateStruct(config)
	if err != nil {
		log.GetLogger().Errorf("the configuration item is incorrect: " + err.Error())
		return nil, fmt.Errorf("failed to verify the config parameter")
	}
	if len(config.NtpServeraddress) > maxNumOfNtpServer {
		return nil, fmt.Errorf("the number of NTP server addresses is greater than %d", maxNumOfNtpServer)
	}
	return config, nil
}

// RegularNtpCheck is used regularly patrol the NTP.
func RegularNtpCheck() {
	if ntpCfg == nil {
		return
	}

	ticker := time.NewTicker(time.Second * time.Duration(ntpCfg.CheckParam.Interval))
	cnt := 0
	failedConCnt := 0
	for {
		select {
		case <-ticker.C:
			cnt, failedConCnt = checkNtp(cnt, failedConCnt)
		case <-ntpStopCh:
			log.GetLogger().Info("ntp check stopped.")
			ticker.Stop()
			return
		}
	}
}

func checkNtp(cnt, failedConCnt int) (int, int) {
	if ntpCfg == nil {
		return cnt, failedConCnt
	}
	ntpTime, err := GetNtpServerTimeEx(ntpCfg.NtpServeraddress)
	if err != nil {
		log.GetLogger().Errorf("Failed to obtain the NTP server time: %s", err.Error())
		failedConCnt = ReportNtpConnAlarm(failedConCnt, serverAlarm)
		return cnt, failedConCnt
	}
	if failedConCnt >= failedConnTimes {
		failedConCnt = RemoveNtpConnAlarm(failedConCnt)
		log.GetLogger().Infof("Succeeded to remove the NTP connection")
	}
	ntpServerTime := ntpTime.Format(timeLayoutFmt) // time string
	ntpOffset := ntpTime.Sub(time.Now())
	log.GetLogger().Infof("Ntp Server time : %s", ntpServerTime)

	if ntpCfg.CheckParam.MaxOffset < int(ntpOffset.Seconds()) ||
		-ntpCfg.CheckParam.MaxOffset > int(ntpOffset.Seconds()) {
		log.GetLogger().Warnf("Maximum ntp offset allowed %d s, offset: %d s",
			ntpCfg.CheckParam.MaxOffset, int(ntpOffset.Seconds()))
		cnt++
		if err = UpdateSystemDate(ntpServerTime); err != nil {
			log.GetLogger().Errorf("set local time error: %s", err.Error())
		}
		if cnt >= ntpCfg.CheckParam.AlarmCnt && alarmFlag == false {
			SendAlarmToOmsvc(offsetAlarm)
			alarmFlag = true
		}
	} else {
		cnt = 0
		if alarmFlag == true {
			SendRemoveAlarmToOmsvc()
			alarmFlag = false
		}
	}
	return cnt, failedConCnt
}

// GetNtpServerTimeEx is used geting ntp server time
func GetNtpServerTimeEx(ntpServeraddress map[string]string) (time.Time, error) {
	for _, host := range ntpServeraddress {
		time, err := GetNtpServerTime(host)
		if err == nil {
			return time, nil
		}
	}
	return time.Time{}, errors.New("all ntp server disconnection")
}

// GetNtpServerTime is used obtain the time of the NTP server.
func GetNtpServerTime(ntpServeraddress string) (time.Time, error) {
	conn, err := net.Dial("udp", ntpServeraddress)
	if err != nil {
		return time.Time{}, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(ntpServerTimeout)); err != nil {
		return time.Time{}, err
	}

	req := &packet{Settings: 0x1B}

	if err := binary.Write(conn, binary.BigEndian, req); err != nil {
		return time.Time{}, err
	}

	rsp := &packet{}
	if err := binary.Read(conn, binary.BigEndian, rsp); err != nil {
		return time.Time{}, err
	}

	secs := float64(rsp.TxTimeSec) - epochOffsetFmt
	nanos := (int64(rsp.TxTimeFrac) * 1e9) >> 32

	showtime := time.Unix(int64(secs), nanos)
	return showtime, nil
}

// UpdateSystemDate is used update local time
func UpdateSystemDate(dateTime string) error {
	commandName := `date -s "` + dateTime + `"`
	cmd := exec.Command("/bin/bash", "-c", commandName)

	// Get Pipeline Input
	output, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	// Running Linux Commands
	if err := cmd.Start(); err != nil {
		return err
	}

	// Read All Outputs
	bytes, err := ioutil.ReadAll(output)
	if err != nil {
		return err
	}

	if err := cmd.Wait(); err != nil {
		return err
	}

	log.GetLogger().Infof("Time After NTP Synchronization: %s", bytes)
	return nil
}
