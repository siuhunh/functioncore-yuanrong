package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"functioncore/pkg/functionbus/common"
)

const (
	defaultProxyCfgFilePath = "config.json"
)

// ProxyCfgIniter -
type ProxyCfgIniter struct {
	k8sClientsetGetter *K8sClientsetGetter

	preferProxyCfgFromConfigmap  bool
	proxyCfgFromConfigmapName    string
	proxyCfgFromConfigmapDataKey string
	proxyDSName                  string

	inited bool
	err    error
}

// NewProxyCfgIniter -
func NewProxyCfgIniter(k8sClientsetGetter *K8sClientsetGetter, cmd *cobra.Command) *ProxyCfgIniter {
	initer := &ProxyCfgIniter{
		k8sClientsetGetter: k8sClientsetGetter,
	}

	flags := cmd.PersistentFlags()

	flags.BoolVar(
		&initer.preferProxyCfgFromConfigmap,
		"prefer-proxy-cfg-from-configmap",
		true,
		"优先从k8s configmap中获取proxy配置文件",
	)
	flags.StringVar(
		&initer.proxyCfgFromConfigmapName,
		"proxy-cfg-from-configmap-name",
		"functiontask-config",
		"从k8s configmap中获取proxy配置configmap名字",
	)
	flags.StringVar(
		&initer.proxyCfgFromConfigmapDataKey,
		"proxy-cfg-from-configmap-data-key",
		"conf.json",
		"从k8s configmap中获取proxy配置configmap data key",
	)
	flags.StringVar(
		&initer.proxyDSName,
		"proxy-ds-name",
		"functiontask",
		"k8s proxy daemonset名字",
	)

	return initer
}

// Init -
func (i *ProxyCfgIniter) Init() (err error) {
	if i.inited {
		return i.err
	}
	defer func() {
		i.inited = true
		i.err = err
	}()

	cs, err := i.k8sClientsetGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get k8s clientset")
	}
	ds, err := cs.AppsV1().DaemonSets("default").Get(context.Background(), i.proxyDSName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get deployment")
	}
	for _, env := range ds.Spec.Template.Spec.Containers[0].Env {
		if v, ok := os.LookupEnv(env.Name); ok {
			fmt.Printf("skip setting env %s to %s, already has value %s\n", env.Name, env.Value, v)
			continue
		}
		os.Setenv(env.Name, env.Value)
	}

	if i.preferProxyCfgFromConfigmap {
		if err := i.initProxyCfgFromConfigmap(cs); err == nil {
			return nil
		}
		fmtError(errors.Wrap(err, "init from configmap"))
		fmt.Fprintf(os.Stderr, "====== fallback ======")
	}

	return errors.Wrap(i.initProxyCfgFromCache(), "init from cache")
}

func (i *ProxyCfgIniter) initProxyCfgFromConfigmap(cs *kubernetes.Clientset) error {
	configmap, err := cs.CoreV1().ConfigMaps("default").Get(context.Background(), i.proxyCfgFromConfigmapName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrapf(err, "get configmap name %s", i.proxyCfgFromConfigmapName)
	}
	content, ok := configmap.Data[i.proxyCfgFromConfigmapDataKey]
	if !ok {
		return fmt.Errorf("unknown data key %s, contains %v", i.proxyCfgFromConfigmapDataKey, Keys(configmap.Data))
	}

	fileName := filepath.Join(cachePath, defaultProxyCfgFilePath)
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, 0644); err != nil {
		return errors.Wrapf(err, "mkdir all %s", dir)
	}
	f, err := os.Create(fileName)
	if err != nil {
		return errors.Wrap(err, "create file")
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return errors.Wrap(err, "write string to file")
	}
	f.Close()

	return i.initProxyCfgFromFile(f.Name())
}

func (i *ProxyCfgIniter) initProxyCfgFromCache() error {
	fileName := filepath.Join(cachePath, defaultProxyCfgFilePath)
	if _, err := os.Stat(fileName); err != nil {
		return errors.Wrap(err, "stat file")
	}
	return i.initProxyCfgFromFile(fileName)
}

func (i *ProxyCfgIniter) initProxyCfgFromFile(fileName string) error {
	if err := common.InitConfig(fileName); err != nil {
		return errors.Wrap(err, "init config")
	}

	return nil
}
