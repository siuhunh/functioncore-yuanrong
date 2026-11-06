/*
 * Copyright (c) 2023 Huawei Technologies Co., Ltd
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

package telemetry

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"net"
	"net/http"
	"sync"
	"time"

	fhttp "github.com/valyala/fasthttp"
	"k8s.io/kubectl/pkg/util/slice"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/logger/log"
)

var (
	// Config -
	Config           Configuration
	createClientOnce sync.Once
	cli              *fhttp.Client
)

const (
	readTimeout = 5 * time.Second
	doTimeout   = 3 * time.Second
	// the total number of telemetry eventType
	eventTypeNum = 3
)

// Configuration -
type Configuration struct {
	Version    string     `json:"version,omitempty"`
	Types      []string   `json:"types,omitempty"`
	Buffer     Buffer     `json:"buffer,omitempty"`
	Subscriber Subscriber `json:"subscriber,omitempty"`
}

// Buffer -
type Buffer struct {
	EventLimit int64 `json:"eventLimit,omitempty"`
	BytesLimit int64 `json:"bytesLimit,omitempty"`
	TimeoutMs  int64 `json:"timeoutMs,omitempty"`
}

// Subscriber -
type Subscriber struct {
	Protocol string `json:"protocol,omitempty"`
	URI      string `json:"URI,omitempty"`
}

// badResponse HTTP request message that does not return 200
type badResponse struct {
	ErrorMessage string `json:"ErrorMessage"`
}

// ParseTelemetryConfig -
func ParseTelemetryConfig(w http.ResponseWriter, r *http.Request) error {
	defer r.Body.Close()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.GetLogger().Errorf("failed to parse telemetry with error: %s", err.Error())
		return err
	}

	if err := json.Unmarshal(b, &Config); err != nil {
		log.GetLogger().Errorf("failed to unmarshal the json, error: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("register telemetry api config: %+v", Config)

	if len(Config.Types) > eventTypeNum || len(Config.Types) == 0 {
		log.GetLogger().Error("telemetry API has unknown event types or event type is empty")
		Config = Configuration{}
		return errors.New("telemetry API has unknown event types or event type is empty, please check your code")
	}
	for _, event := range Config.Types {
		if event != constants.TelemetryPlatformType && event != constants.TelemetryExtensionType &&
			event != constants.TelemetryFunctionType {
			log.GetLogger().Error("telemetry API has unknown event types")
			Config = Configuration{}
			return errors.New("telemetry API has unknown event types, please check your code")
		}
	}
	return nil
}

func sendTelemetryMessage(bytes []byte, traceID string) {
	req := fhttp.AcquireRequest()
	req.SetRequestURI(Config.Subscriber.URI)
	req.SetBody(bytes)

	defer fhttp.ReleaseRequest(req)
	response := fhttp.AcquireResponse()
	defer fhttp.ReleaseResponse(response)

	req.Header.SetContentType("application/json")
	req.Header.SetMethod("POST")
	req.Header.Set(constants.HeaderTraceID, traceID)

	createClientOnce.Do(func() {
		cli = &fhttp.Client{
			ReadTimeout: readTimeout,
			Dial: func(addr string) (conn net.Conn, e error) {
				return fhttp.DialTimeout(addr, doTimeout)
			},
		}
	})
	if cli == nil {
		log.GetLogger().Error("telemetry failed to send message, client is nil")
		return
	}
	err := cli.DoTimeout(req, response, doTimeout)
	if err != nil {
		log.GetLogger().Errorf("failed to send telemetry request to %s, traceID(%s): %s",
			Config.Subscriber.URI, traceID, err.Error())
		return
	}
	if response.Header.StatusCode() != fhttp.StatusOK {
		log.GetLogger().Errorf("failed to send telemetry request , traceID:%s, respCode:%d, respBody:%s",
			traceID, response.Header.StatusCode(), response.Body())
		return
	}
}

// SendEvent build telemetry event
func SendEvent(eventData Event) {
	containsType := slice.ContainsString(Config.Types, eventData.Type, nil)
	if !containsType {
		return
	}
	eventBytes := parseEvent(eventData)
	sendTelemetryMessage(eventBytes, eventData.TraceID)
}

func parseEvent(eventData Event) []byte {
	bytes, err := json.Marshal(eventData)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal eventData, error: %s", err.Error())
		return nil
	}

	var event interface{}
	switch eventData.Name {
	case constants.TelemetryInitStart, constants.TelemetryStart, constants.TelemetryRestoreStart:
		event = &StartEvent{}
	case constants.TelemetryInitRuntimeDone, constants.TelemetryRuntimeDone, constants.TelemetryRestoreRuntimeDone:
		event = &RuntimeDoneEvent{}
	case constants.TelemetryInitReport, constants.TelemetryRestoreReport, constants.TelemetryReport:
		event = &ReportEvent{}
	case constants.TelemetryFunctionType:
		event = &FunctionEvent{}
	case constants.TelemetryExtensionType:
		event = &ExtensionEvent{}
	default:
		log.GetLogger().Warnf("telemetry event name is an unknown type")
	}

	err = json.Unmarshal(bytes, event)
	if err != nil {
		log.GetLogger().Warnf("failed to unmarshal eventData, error: %s", err.Error())
		return nil
	}
	result, err := json.Marshal(event)
	if err != nil {
		log.GetLogger().Warnf("failed to marshal eventData, error: %s", err.Error())
		return nil
	}
	return result
}

// OpenTelemetryAPI open openTelemetry api
func OpenTelemetryAPI(w http.ResponseWriter, r *http.Request) {
	log.GetLogger().Infof("start to open OpenTelemetryAPI")
	err := ParseTelemetryConfig(w, r)
	if err == nil {
		w.WriteHeader(http.StatusOK)
		return
	}
	w.WriteHeader(http.StatusBadRequest)
	w.Header().Add(constants.ContentType, constants.ApplicationJSON)

	response := badResponse{
		ErrorMessage: err.Error(),
	}

	rsp, err := json.Marshal(response)
	if err != nil {
		log.GetLogger().Errorf("failed to marshal badResponse, error: %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = w.Write(rsp)
	if err != nil {
		log.GetLogger().Errorf("failed to write the response, error: %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
	}
}
