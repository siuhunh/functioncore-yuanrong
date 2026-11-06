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

	"functioncore/pkg/worker-manager/config"
)

const (
	defaultWorkerManagerCfgFilePath = "worker-manager.conf"
)

// WorkerManagerCfgGetter -
type WorkerManagerCfgGetter struct {
	k8sClientsetGetter *K8sClientsetGetter

	preferWorkerManagerCfgFromConfigmap  bool
	workerManagerCfgFromConfigmapName    string
	workerManagerCfgFromConfigmapDataKey string
	workerManagerDeployName              string

	x   *config.FaasConfig
	err error
}

// NewWorkerManagerCfgGetter -
func NewWorkerManagerCfgGetter(
	k8sClientsetGetter *K8sClientsetGetter,
	cmd *cobra.Command) *WorkerManagerCfgGetter {

	getter := &WorkerManagerCfgGetter{
		k8sClientsetGetter: k8sClientsetGetter,
	}

	flags := cmd.PersistentFlags()

	flags.BoolVar(
		&getter.preferWorkerManagerCfgFromConfigmap,
		"prefer-worker-manager-cfg-from-configmap",
		true,
		"优先从k8s configmap中获取worker-manager配置文件",
	)
	flags.StringVar(
		&getter.workerManagerCfgFromConfigmapName,
		"worker-manager-cfg-from-configmap-name",
		"worker-manager-service-configmap",
		"从k8s configmap中获取worker-manager配置configmap名字",
	)
	flags.StringVar(
		&getter.workerManagerCfgFromConfigmapDataKey,
		"worker-manager-cfg-from-configmap-data-key",
		"worker-manager-config",
		"从k8s configmap中获取worker-manager配置configmap data key",
	)
	flags.StringVar(
		&getter.workerManagerDeployName,
		"worker-manager-deploy-name",
		"worker-manager",
		"k8s worker-manager deployment名字",
	)

	return getter
}

// Get -
func (g *WorkerManagerCfgGetter) Get() (cfg *config.FaasConfig, err error) {
	if g.x != nil {
		return g.x, g.err
	}
	defer func() {
		g.x, g.err = cfg, err
	}()

	cs, err := g.k8sClientsetGetter.Get()
	if err != nil {
		return nil, errors.Wrap(err, "get k8s clientset")
	}

	deploy, err := cs.AppsV1().Deployments("default").Get(context.Background(), g.workerManagerDeployName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "get deployment")
	}
	for _, env := range deploy.Spec.Template.Spec.Containers[0].Env {
		if v, ok := os.LookupEnv(env.Name); ok {
			fmt.Printf("skip setting env %s to %s, already has value %s\n", env.Name, env.Value, v)
			continue
		}
		os.Setenv(env.Name, env.Value)
	}

	if g.preferWorkerManagerCfgFromConfigmap {
		cfg, err := g.getWorkerManagerConfigFromConfigmap(cs)
		if err == nil {
			return cfg, nil
		}
		fmtError(errors.Wrap(err, "init from configmap"))
		fmt.Fprintf(os.Stderr, "====== fallback ======")
	}

	cfg, err = g.getWorkerManagerConfigFromCache()
	if err != nil {
		return nil, errors.Wrap(err, "init from cache")
	}
	return cfg, nil
}

func (g *WorkerManagerCfgGetter) getWorkerManagerConfigFromConfigmap(cs *kubernetes.Clientset) (*config.FaasConfig, error) {
	configmap, err := cs.CoreV1().ConfigMaps("default").Get(context.Background(), g.workerManagerCfgFromConfigmapName, metav1.GetOptions{})
	if err != nil {
		return nil, errors.Wrapf(err, "get configmap name %s", g.workerManagerCfgFromConfigmapName)
	}
	content, ok := configmap.Data[g.workerManagerCfgFromConfigmapDataKey]
	if !ok {
		return nil, fmt.Errorf("unknown data key %s, contains %v", g.workerManagerCfgFromConfigmapDataKey, Keys(configmap.Data))
	}

	fileName := filepath.Join(cachePath, defaultWorkerManagerCfgFilePath)
	dir := filepath.Dir(fileName)
	if err := os.MkdirAll(dir, 0644); err != nil {
		return nil, errors.Wrapf(err, "mkdir all %s", dir)
	}
	f, err := os.Create(fileName)
	if err != nil {
		return nil, errors.Wrap(err, "create file")
	}
	if _, err := f.WriteString(content); err != nil {
		f.Close()
		return nil, errors.Wrap(err, "write string to file")
	}
	f.Close()

	return g.getWorkerManagerConfig(f.Name())
}

func (g *WorkerManagerCfgGetter) getWorkerManagerConfigFromCache() (*config.FaasConfig, error) {
	fileName := filepath.Join(cachePath, defaultWorkerManagerCfgFilePath)
	return g.getWorkerManagerConfig(fileName)
}

func (g *WorkerManagerCfgGetter) getWorkerManagerConfig(fileName string) (*config.FaasConfig, error) {
	config.InitConfigPath(fileName)

	cfg, err := config.GetConfig()
	if err != nil {
		return nil, errors.Wrap(err, "get config")
	}

	return cfg, nil
}
