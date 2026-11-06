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

// Package localauth authenticates requests by local configmaps
package localauth

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/utils"
)

const (
	modeSDK = "SDKMode"
	modeHWS = "HWSMode"
	// the difference limit of a timestamp
	defaultTimestampDiffLimit = 5
	// 7 days
	maxTimestampDiffLimit = 10080
	maxHeaderLength       = 20
	minLengthOfAuthValue  = 2
	base                  = 10
	bitSize               = 64
)

var timestampDiffLimit = getTimestampDiffLimit()

type modeOptions struct {
	authHeaderPrefix string
	timeFormat       string
	shortTimeFormat  string
	terminalString   string
	name             string
	date             string
}

// Signer is a struct of
type Signer struct {
	signTime    time.Time
	serviceName string
	region      string
	modeOption  *modeOptions
}

// AuthConfig represents configurations of local auth
type AuthConfig struct {
	AKey     string `json:"aKey" yaml:"aKey" valid:"optional"`
	SKey     string `json:"sKey" yaml:"sKey" valid:"optional"`
	Duration int    `json:"duration" yaml:"duration" valid:"optional"`
}

// Authentication represents aKey and sKey Decrypted from ak and sk
type Authentication struct {
	AKey []byte
	SKey []byte
}

// signLocalAuthRequest returns the authentication header
func signLocalAuthRequest(rawURL, timeStamp, appID string, key *Authentication, data []byte) (string, []byte) {
	signer := getSigner("SDKMode", "", "")
	timeStampInt, err := strconv.ParseInt(timeStamp, base, bitSize)
	if err != nil {
		log.GetLogger().Errorf("failed to parse the timestamp string")
		return "", data
	}
	signer.signTime = time.Unix(timeStampInt, 0)
	// default text of data
	if data == nil {
		data = []byte(`signature verification`)
	}
	header := make(map[string][]string, maxHeaderLength)
	header["Content-Type"] = []string{"application/json"}

	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		log.GetLogger().Errorf("failed to parse a URL")
		return "", data
	}
	request := &http.Request{Method: "POST", URL: parsedURL, Header: header}
	signerHeader := signer.sign(request, key.AKey, key.SKey, data, appID)
	return signerHeader["X-Identity-Sign"], data
}

func getSigner(mode, serviceName, region string) *Signer {
	var opt *modeOptions
	if mode == modeSDK {
		opt = sdkMod
	} else {
		opt = hwsMode
	}
	return &Signer{
		signTime:    time.Now(),
		serviceName: serviceName,
		region:      region,
		modeOption:  opt,
	}
}

var sdkMod = &modeOptions{
	authHeaderPrefix: "SDK-HMAC-SHA256",
	timeFormat:       "20060102T150405Z",
	shortTimeFormat:  "20060102",
	terminalString:   "sdk_request",
	name:             "SDK",
	date:             "X-Sdk-Date",
}

var hwsMode = &modeOptions{
	authHeaderPrefix: "HWS-HMAC-SHA256",
	timeFormat:       "20060102T150405Z",
	shortTimeFormat:  "20060102",
	terminalString:   "hws_request",
	name:             "HWS",
	date:             "X-Hws-Date",
}

func (sig *Signer) sign(request *http.Request, aKey, sKey []byte, body []byte,
	appID string) map[string]string {
	header := map[string]string{}
	request.Header.Add(sig.modeOption.date, sig.signTime.UTC().Format(sig.modeOption.timeFormat))
	contentSha256 := makeSha256Hex(body)
	canonicalString := sig.buildCanonicalRequest(request, contentSha256)
	stringToSign := sig.buildStringToSign(canonicalString)
	signatureStr := sig.buildSignature(sKey, stringToSign)
	credentialString := sig.buildCredentialString()
	signedHeaders := sig.buildSignedHeadersString(request)
	aKeyString := string(aKey)
	utils.ClearByteMemory(aKey)
	parts := []string{
		sig.modeOption.authHeaderPrefix + " Credential=" + aKeyString + "/" + credentialString,
		"SignedHeaders=" + signedHeaders,
		"Signature=" + signatureStr,
	}
	if appID != "" {
		parts = append(parts, "appid="+appID)
	}
	utils.ClearStringMemory(aKeyString)

	signResult := strings.Join(parts, ", ")
	header["host"] = request.Host
	header[sig.modeOption.date] = sig.signTime.UTC().Format(sig.modeOption.timeFormat)
	header["Content-Type"] = "application/json;charset=UTF-8"
	header["Accept"] = "application/json"
	header["X-Identity-Sign"] = signResult
	return header
}

// buildSignature generate a signature with request and secret key
func (sig *Signer) buildSignature(sKey []byte, stringtoSign string) string {
	var secretBuf bytes.Buffer
	secretBuf.Write([]byte(sig.modeOption.name))
	secretBuf.Write(sKey)
	utils.ClearByteMemory(sKey)
	sigTime := []byte(sig.signTime.UTC().Format(sig.modeOption.shortTimeFormat))
	date := makeHmac(secretBuf.Bytes(), sigTime)
	secretBuf.Reset()
	region := makeHmac(date, []byte(sig.region))
	service := makeHmac(region, []byte(sig.serviceName))
	credentials := makeHmac(service, []byte(sig.modeOption.terminalString))
	toSignature := makeHmac(credentials, []byte(stringtoSign))
	signature := hex.EncodeToString(toSignature)
	return signature
}

// buildStringToSign prepare data for building signature
func (sig *Signer) buildStringToSign(canonicalString string) string {
	stringToSign := strings.Join([]string{
		sig.modeOption.authHeaderPrefix,
		sig.signTime.UTC().Format(sig.modeOption.timeFormat),
		sig.buildCredentialString(),
		hex.EncodeToString(makeSha256([]byte(canonicalString))),
	}, "\n")
	return stringToSign
}

// buildCanonicalRequest converts the request info into canonical format
func (sig *Signer) buildCanonicalRequest(request *http.Request, hexbody string) string {
	canonicalHeadersOut := sig.buildCanonicalHeaders(request)
	signedHeaders := sig.buildSignedHeadersString(request)
	canonicalRequestStr := strings.Join([]string{
		request.Method,
		request.URL.Path + "/",
		request.URL.RawQuery,
		canonicalHeadersOut,
		signedHeaders,
		hexbody,
	}, "\n")
	return canonicalRequestStr
}

// buildCanonicalHeaders generate canonical headers
func (sig *Signer) buildCanonicalHeaders(request *http.Request) string {
	var headers []string

	for header := range request.Header {
		standardized := strings.ToLower(strings.TrimSpace(header))
		headers = append(headers, standardized)
	}
	sort.Strings(headers)

	for i, header := range headers {
		headers[i] = header + ":" + strings.Replace(request.Header.Get(header), "\n", " ", -1)
	}

	if len(headers) > 0 {
		return strings.Join(headers, "\n") + "\n"
	}
	return ""
}

// buildSignedHeadersString convert the header in request to a certain format
func (sig *Signer) buildSignedHeadersString(request *http.Request) string {
	var headers []string
	for header := range request.Header {
		headers = append(headers, strings.ToLower(header))
	}
	sort.Strings(headers)
	return strings.Join(headers, ";")
}

// buildCredentialString add date and several other information to signature header
func (sig *Signer) buildCredentialString() string {
	credentialString := strings.Join([]string{
		sig.signTime.UTC().Format(sig.modeOption.shortTimeFormat),
		sig.region,
		sig.serviceName,
		sig.modeOption.terminalString,
	}, "/")
	return credentialString
}

// makeHmac convert data into sha256 format with certain key
func makeHmac(key []byte, data []byte) []byte {
	hash := hmac.New(sha256.New, key)
	_, err := hash.Write(data)
	if err != nil {
		log.GetLogger().Errorf("failed to write in makeHmac, error: %s", err.Error())
	}
	return hash.Sum(nil)

}

// makeHmac convert data into sha256 format
func makeSha256(data []byte) []byte {
	hash := sha256.New()
	_, err := hash.Write(data)
	if err != nil {
		log.GetLogger().Errorf("failed to write in makeSha256, error: %s", err.Error())
	}
	return hash.Sum(nil)
}

// makeHmac convert data into Hex format
func makeSha256Hex(data []byte) string {
	hash := sha256.New()
	_, err := hash.Write(data)
	if err != nil {
		log.GetLogger().Errorf("failed to write in makeSha256Hex, error: %s", err.Error())
	}
	md := hash.Sum(nil)
	hexBody := hex.EncodeToString(md)
	return hexBody
}

func getTimestampDiffLimit() float64 {
	var tsDiffLimit float64
	envTimestampDiffLimit, err := strconv.Atoi(os.Getenv("AUTH_VALID_TIME_MINUTE"))
	if err == nil && envTimestampDiffLimit > 0 && envTimestampDiffLimit <= maxTimestampDiffLimit {
		tsDiffLimit = float64(envTimestampDiffLimit)
	} else {
		tsDiffLimit = float64(defaultTimestampDiffLimit)
	}
	log.GetLogger().Infof("current timestampDiffLimit is %f", tsDiffLimit)
	return tsDiffLimit
}

// AuthCheckLocally authenticates requests by local auth
func AuthCheckLocally(ak string, sk string, requestSign string, timestamp string, duration int) error {
	if len(requestSign) == 0 {
		return fmt.Errorf("authentication string is nil")
	}
	curTime := time.Now().Unix()
	timeUnix, err := strconv.ParseInt(timestamp, base, bitSize)
	if err != nil {
		return fmt.Errorf("invalid timestamp")
	}
	// the default timestamp limit is 5 minutes
	if math.Abs(float64(curTime-timeUnix)) >= timestampDiffLimit*time.Minute.Seconds() {
		return fmt.Errorf("the request is timeout")
	}
	appID, err := getAppIDFromRequestSign(requestSign)
	if err != nil {
		return err
	}
	_, exist, err := GetLocalAuthCache(ak, sk, appID, duration).GetSignForReceiver(requestSign)
	if err != nil {
		log.GetLogger().Errorf("failed to get sign from receiver cache")
		return err
	}
	if exist {
		return nil
	}
	aKey, sKey, err := DecryptKeys(ak, sk)
	if err != nil {
		utils.ClearByteMemory(aKey)
		utils.ClearByteMemory(sKey)
		return err
	}
	key := &Authentication{
		AKey: aKey,
		SKey: sKey,
	}
	var data []byte
	signature, _ := signLocalAuthRequest("", timestamp, appID, key, data)
	utils.ClearByteMemory(aKey)
	utils.ClearByteMemory(sKey)
	if signature == "" || signature != requestSign {
		return fmt.Errorf("auth check failed")
	}
	if err := GetLocalAuthCache(ak, sk, appID, duration).updateReceiver(signature); err != nil {
		log.GetLogger().Errorf("failed to update receiver cache")
		return err
	}
	return nil
}

func getAppIDFromRequestSign(sign string) (string, error) {
	arrays := strings.Split(sign, "appid=")
	if len(arrays) < minLengthOfAuthValue {
		return "", fmt.Errorf("failed to parse authorization appid= %s", "*****")
	}
	arrays = strings.Split(arrays[1], ", ")
	return arrays[0], nil
}

// SignLocally makes signatures by local auth
func SignLocally(ak, sk, appID string, duration int) (string, string) {
	t, auth, err := GetLocalAuthCache(ak, sk, appID, duration).GetSignForSender()
	if err != nil {
		var data []byte
		log.GetLogger().Warnf("failed to get sender cache: %s", err.Error())
		return CreateAuthorization(ak, sk, "", appID, data)
	}
	return auth, t
}

// SignOMSVC make signatures for request send to OMSVC
func SignOMSVC(ak, sk, url string, data []byte) (string, string) {
	return CreateAuthorization(ak, sk, url, "", data)
}

// CreateAuthorization create Authentication Information
func CreateAuthorization(ak, sk, url, appID string, data []byte) (string, string) {
	timestamp := strconv.FormatInt(time.Now().Unix(), base)
	aKey, sKey, err := DecryptKeys(ak, sk)
	if err != nil {
		utils.ClearByteMemory(aKey)
		utils.ClearByteMemory(sKey)
		log.GetLogger().Errorf("failed to decrypt SKey when create auth, error: %s", err.Error())
		return "", ""
	}
	key := &Authentication{
		AKey: aKey,
		SKey: sKey,
	}
	authorization, _ := signLocalAuthRequest(url, timestamp, appID, key, data)
	utils.ClearByteMemory(aKey)
	utils.ClearByteMemory(sKey)
	return authorization, timestamp
}
