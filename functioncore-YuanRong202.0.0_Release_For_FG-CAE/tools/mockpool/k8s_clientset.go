package main

import (
	"os"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	defaultK8sConfigFile = "$HOME/.kube/config"
)

// K8sClientsetGetter -
type K8sClientsetGetter struct {
	configFile string

	x   *kubernetes.Clientset
	err error
}

// NewK8sClientsetGetter -
func NewK8sClientsetGetter(cmd *cobra.Command) *K8sClientsetGetter {
	getter := &K8sClientsetGetter{}

	flags := cmd.PersistentFlags()
	flags.StringVar(
		&getter.configFile,
		"k8s-config-file",
		defaultK8sConfigFile,
		"k8s config文件路径",
	)

	return getter
}

// Get -
func (g *K8sClientsetGetter) Get() (cs *kubernetes.Clientset, err error) {
	if g.x != nil {
		return g.x, g.err
	}
	defer func() {
		g.x, g.err = cs, err
	}()

	kubeCfg, err := clientcmd.BuildConfigFromFlags("", os.ExpandEnv(g.configFile))
	if err != nil {
		return nil, errors.Wrap(err, "build config from flags")
	}

	cs, err = kubernetes.NewForConfig(kubeCfg)
	if err != nil {
		return nil, errors.Wrap(err, "new for config")
	}

	return cs, nil
}
