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

package monitoring

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

const (
	swrHealthCheckIntervalTime = 5 * time.Minute
	swrRetryTimes              = 3
	swrTimeout                 = 3 * time.Second
)

func swrHealthCheckAndReport(omvscAddr, ak, sk, swrAPI string, stopCh <-chan struct{}) {
	if stopCh == nil {
		return
	}
	cli := &http.Client{}
	for {
		select {
		case <-stopCh:
			log.GetLogger().Infof("graceful exit")
			return
		default:
			time.Sleep(swrHealthCheckIntervalTime)
			if err := checkSWRHealth(cli, omvscAddr, ak, sk, swrAPI); err != nil {
				// 5min interval without suppression
				alarm.GetClient().ReportAlarm(alarm.SWRConnection, fmt.Sprintf("error msg=%s,", err.Error()))
				continue
			}
			alarm.GetClient().ClearAlarm(alarm.SWRConnection)
		}
	}
}

func checkSWRHealth(cli *http.Client, omvscAddr, ak, sk, swrAPI string) error {
	token, err := getTokenByAddresses(omvscAddr, ak, sk)
	if err != nil {
		log.GetLogger().Errorf("failed to get token: %s", err.Error())
		return errors.New("failed to get token from omsvc")
	}
	defer utils.ClearStringMemory(token)

	url := fmt.Sprintf("https://%s/v2/manage/utils/secret", swrAPI)
	for i := 0; i < swrRetryTimes; i++ {
		var retry bool
		retry, err = sendSWRRequest(cli, url, token)
		if !retry {
			return err
		}
		log.GetLogger().Errorf("retry to send swr request, i: %d, %s", i, err)
	}
	return err
}

func sendSWRRequest(cli *http.Client, url, token string) (bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), swrTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		log.GetLogger().Errorf("failed to make alarm request, err %s", err.Error())
		return true, err
	}
	req.Header.Add("X-Auth-Token", token)

	resp, err := cli.Do(req)
	if err != nil {
		err := fmt.Errorf("failed to request swr %s, %s", url, err.Error())
		log.GetLogger().Errorf(err.Error())
		return true, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusTooManyRequests {
		return false, fmt.Errorf("failed to request swr %s. The throttling threshold has been reached", url)
	}
	if resp.StatusCode != http.StatusOK {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			err := fmt.Errorf("failed to request swr %s, statusCode=%d, %s", url, resp.StatusCode, err.Error())
			log.GetLogger().Errorf(err.Error())
			return true, err
		}

		log.GetLogger().Errorf("failed to request swr %s, statusCode=%d, message=%s", url, resp.StatusCode, body)
		return true, fmt.Errorf("failed to request swr %s, statusCode=%d", url, resp.StatusCode)
	}
	return false, nil
}

type authToken struct {
	RawToken string
}

func getTokenByAddresses(omsvcAddr, ak, sk string) (string, error) {
	omsvcAddresses := utils.ShuffleOneArray(strings.Split(omsvcAddr, ","))
	var err error
	for _, address := range omsvcAddresses {
		token, getTokenErr := getToken(address, ak, sk)
		if getTokenErr != nil {
			err = getTokenErr
			log.GetLogger().Errorf("failed to get token from omsvc address: %s, err: %s ", address, err.Error())
			// Failed to retry the next URL.
			continue
		}
		log.GetLogger().Infof("succeed to get token from omsvc %s", address)
		return token, nil
	}
	return "", err
}

func getToken(omsvcAddr, ak, sk string) (string, error) {
	uri := "/v2/fgs/om/token"
	requestURI := fmt.Sprintf("https://%s%s", omsvcAddr, uri)
	body := []byte(`{"token_level": "project", "account_type": "resource"}`)
	req, err := http.NewRequest(http.MethodPost, requestURI, bytes.NewBuffer(body))
	if err != nil {
		log.GetLogger().Errorf("failed to make token request: %s", err.Error())
		return "", err
	}
	authorization, timestamp := localauth.SignOMSVC(ak, sk, requestURI, body)
	req.Header.Set(alarm.HeaderAlarmAuthorization, authorization)
	req.Header.Set(alarm.HeaderAlarmAuthTimestamp, timestamp)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.GetLogger().Errorf("failed to send token request: %s", err.Error())
		return "", err
	}
	body, err = ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		log.GetLogger().Errorf("failed to read body, statusCode: %s", resp.StatusCode)
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		log.GetLogger().Errorf("failed to send token, server returns statusCode: %d, body: %s", resp.StatusCode, body)
		return "", fmt.Errorf("unexpected statusCode: %d", resp.StatusCode)
	}

	authToken := authToken{}
	if err := json.Unmarshal(body, &authToken); err != nil {
		log.GetLogger().Errorf("failed to unmarshal body, %s, body: %s", err.Error(), body)
		return "", err
	}

	return authToken.RawToken, nil
}
