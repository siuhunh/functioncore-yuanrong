package main

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	fhttp "github.com/valyala/fasthttp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/worker-manager/utils"
)

// CheckpointCmd -
type CheckpointCmd struct {
	k8sClientsetGetter     *K8sClientsetGetter
	workerManagerCfgGetter *WorkerManagerCfgGetter
	tlsIniter              *TLSIniter
	localAuthIniter        *LocalAuthIniter

	podName string
}

// NewCheckpointCmd -
func NewCheckpointCmd(
	k8sClientsetGetter *K8sClientsetGetter,
	workerManagerCfgGetter *WorkerManagerCfgGetter,
	tlsIniter *TLSIniter,
	localAuthIniter *LocalAuthIniter) *CheckpointCmd {

	return &CheckpointCmd{
		k8sClientsetGetter:     k8sClientsetGetter,
		workerManagerCfgGetter: workerManagerCfgGetter,
		tlsIniter:              tlsIniter,
		localAuthIniter:        localAuthIniter,
	}
}

// ApplyCobra -
func (c *CheckpointCmd) ApplyCobra(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(
		&c.podName,
		"pod-name",
		"",
		"pod名",
	)
	cmd.MarkFlagRequired("pod-name")
}

// Run -
func (c *CheckpointCmd) Run() error {
	cs, err := c.k8sClientsetGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get k8s clientset")
	}
	pod, err := cs.CoreV1().Pods("default").Get(context.Background(), c.podName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get pod")
	}

	cfg, err := c.workerManagerCfgGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get worker manager cfg")
	}
	enabled := cfg.HTTPSConfig.HTTPSEnable
	if enabled {
		if err := c.tlsIniter.Init(); err != nil {
			return errors.Wrap(err, "init tls")
		}

		if err := tls.InitTLSConfig(cfg.HTTPSConfig.TLSProtocol, cfg.HTTPSConfig.TLSCiphers); err != nil {
			return errors.Wrap(err, "init tls config")
		}
	}

	if err := c.localAuthIniter.Init(); err != nil {
		return errors.Wrap(err, "init local auth")
	}

	req, response := fhttp.AcquireRequest(), fhttp.AcquireResponse()
	scheme := "https"
	if !cfg.HTTPSConfig.HTTPSEnable {
		scheme = "http"
	}
	req.SetRequestURI(fmt.Sprintf("%s//%s:%s/checkpoint", scheme, pod.Status.PodIP, "8081"))
	req.Header.SetMethod(fhttp.MethodGet)
	req.Header.Set(constants.HeaderTraceID, "mockpool")

	req.Header.ResetConnectionClose()
	authorization, timestamp := localauth.SignLocally(cfg.LocalAuth.AKey, cfg.LocalAuth.SKey,
		utils.AppID, cfg.LocalAuth.Duration)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.Set(constants.Authorization, authorization)
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)

	workerClient := newWorkerClient()
	if err := workerClient.DoTimeout(req, response, 3*time.Second); err != nil {
		return errors.Wrap(err, "do timeout")
	}

	if response.StatusCode() != http.StatusOK {
		fmt.Println("checkpoint failed with status code:", response.StatusCode())
	} else {
		fmt.Println("checkpoint successfully")
	}
	fmt.Println(string(response.Body()))

	return nil
}
