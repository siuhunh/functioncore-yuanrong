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

package tokenrotation

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"functioncore/pkg/common/utils"
)

const (
	idTokenPath    = "/var/run/secrets/tokens/csms-token"
	requestTimeout = 5 * time.Second

	headerIdentityProvider = "X-Idp-Id"
	headerSubjectToken     = "X-Subject-Token"
	headerAuthToken        = "X-Auth-Token"
	tempTokenTTLSeconds    = 86400
)

var (
	httpClient *http.Client
	once       sync.Once
)

func getHTTPClient() *http.Client {
	once.Do(func() {
		httpClient = &http.Client{
			Timeout: requestTimeout,
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
		}
	})
	return httpClient
}

// CredentialInfo message struct from IAM
type CredentialInfo struct {
	Token TempToken `json:"credential"`
}

// TempToken message son struct from IAM
type TempToken struct {
	Access        string `json:"access"`
	Secret        string `json:"secret"`
	SecurityToken string `json:"securitytoken"`
}

func getTokenFromIAM() (TempToken, error) {
	idToken, err := readRotatedIDToken()
	if err != nil {
		return TempToken{}, fmt.Errorf("failed to read id token: %s", err.Error())
	}

	iamToken, err := getIAMToken(idToken)
	utils.ClearByteMemory(idToken)
	if err != nil {
		return TempToken{}, fmt.Errorf("failed to get iam token: %s", err.Error())
	}

	if config.EnableTokenByAgency {
		if iamToken, err = getRealIAMTokenByAgency(iamToken); err != nil {
			return TempToken{}, fmt.Errorf("failed to get IAM token by agency: %s", err.Error())
		}
	}
	tempToken, err := getTempTokenByIAMToken(iamToken)
	utils.ClearStringMemory(iamToken)
	if err != nil {
		return TempToken{}, fmt.Errorf("failed to get temp token by iam token: %s", err.Error())
	}
	return tempToken, nil
}

func readRotatedIDToken() ([]byte, error) {
	file, err := os.Open(idTokenPath)
	if err != nil {
		return []byte(""), err
	}
	defer file.Close()

	idToken, err := ioutil.ReadAll(file)
	if err != nil {
		return []byte(""), err
	}
	return idToken, nil
}

func getIAMToken(idToken []byte) (string, error) {
	tokenBody := fmt.Sprintf(`{"auth":{"id_token":{"id":"%s"},"scope":{"project":{"id":"%s","name":"%s"}}}}`,
		string(idToken), config.ProjectID, regionID)
	defer utils.ClearStringMemory(tokenBody)
	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/v3.0/OS-AUTH/id-token/tokens",
		config.IAMHostName), bytes.NewBuffer([]byte(tokenBody)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerIdentityProvider, config.IdentityProvider)
	resp, err := getHTTPClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("failed to request iam with status code: %d", resp.StatusCode)
	}
	return resp.Header.Get(headerSubjectToken), nil
}

func getRealIAMTokenByAgency(sonIAMToken string) (string, error) {
	tokenBody := fmt.Sprintf(`{"auth":{"identity":{"methods":["assume_role"],"assume_role":{"agency_name":"%s",
"domain_name":"%s"}},"scope":{"domain":{"name": "%s"}}}}`,
		config.TokenAgencyName, config.TokenAgencyDomain, config.TokenAgencyDomain)
	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/v3/auth/tokens",
		config.IAMHostName), bytes.NewBuffer([]byte(tokenBody)))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerAuthToken, sonIAMToken)
	resp, err := getHTTPClient().Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return "", fmt.Errorf("failed to request iam by agency with status code: %d", resp.StatusCode)
	}
	return resp.Header.Get(headerSubjectToken), nil
}

func getTempTokenByIAMToken(iamToken string) (TempToken, error) {
	authBody := fmt.Sprintf(`{"auth":{"identity":{"methods":["token"],"token":{"duration_seconds":%d}}}}`,
		tempTokenTTLSeconds)
	req, err := http.NewRequest("POST", fmt.Sprintf("https://%s/v3.0/OS-CREDENTIAL/securitytokens",
		config.IAMHostName), bytes.NewBuffer([]byte(authBody)))
	if err != nil {
		return TempToken{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(headerAuthToken, iamToken)
	resp, err := getHTTPClient().Do(req)
	if err != nil {
		return TempToken{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		return TempToken{}, fmt.Errorf("failed to request iam with status code: %d", resp.StatusCode)
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return TempToken{}, err
	}

	credentialInfo := CredentialInfo{}
	if err = json.Unmarshal(respBody, &credentialInfo); err != nil {
		return TempToken{}, err
	}
	return credentialInfo.Token, nil
}
