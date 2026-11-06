package main

import (
	"context"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PProfCmd -
type PProfCmd struct {
	k8sClientsetGetter *K8sClientsetGetter

	podName      string
	pprofPort    int
	pprofType    string
	pprofSeconds int
	pprofOutput  string
}

// NewPprofCmd -
func NewPprofCmd(k8sClientsetGetter *K8sClientsetGetter) *PProfCmd {
	return &PProfCmd{
		k8sClientsetGetter: k8sClientsetGetter,
	}
}

// ApplyCobra -
func (p *PProfCmd) ApplyCobra(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(
		&p.podName,
		"pod-name",
		"",
		"pod名",
	)
	cmd.MarkFlagRequired("pod-name")
	p.AddExtraFlags(flags)
}

// AddExtraFlags -
func (p *PProfCmd) AddExtraFlags(flags *pflag.FlagSet) {
	flags.IntVar(
		&p.pprofPort,
		"pprof-port",
		6060,
		"pprof监听端口",
	)

	flags.StringVar(
		&p.pprofType,
		"pprof-type",
		"profile",
		"pprof prof类型, 有heap, profile, block, mutex, trace等, 具体参照pprof文档",
	)

	flags.IntVar(
		&p.pprofSeconds,
		"pprof-seconds",
		0,
		"pprof prof秒数(heap不生效)",
	)

	flags.StringVar(
		&p.pprofOutput,
		"pprof-output",
		"",
		"生成的prof文件, 默认为pod名 + '_' + pprof-type + '.prof'",
	)
}

// Run -
func (p *PProfCmd) Run() error {
	cs, err := p.k8sClientsetGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get k8s clientset")
	}
	pod, err := cs.CoreV1().Pods("default").Get(context.Background(), p.podName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get pod")
	}

	url := "http://" + pod.Status.PodIP + ":" + strconv.Itoa(p.pprofPort) + "/debug/pprof/" + p.pprofType
	if p.pprofSeconds != 0 && p.pprofType != "heap" {
		url += "?seconds=" + strconv.Itoa(p.pprofSeconds)
	}

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return errors.Wrapf(err, "new request url: %s", url)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return errors.Wrapf(err, "do request url: %s", url)
	}
	defer resp.Body.Close()

	if p.pprofOutput == "" {
		p.pprofOutput = pod.Name + "_" + p.pprofType + ".prof"
	}
	f, err := os.Create(p.pprofOutput)
	if err != nil {
		return errors.Wrapf(err, "create file: %s", p.pprofOutput)
	}
	defer f.Close()

	_, err = io.Copy(f, resp.Body)
	if err != nil {
		return errors.Wrapf(err, "write profile data to file: %s, url: %s", p.pprofOutput, url)
	}

	return nil
}
