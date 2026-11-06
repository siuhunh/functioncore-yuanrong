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

	"functioncore/pkg/common/etcd3"
)

const (
	defaultCAPath = "ca"
)

// CAIniter -
type CAIniter struct {
	k8sClientsetGetter *K8sClientsetGetter

	preferCAFromSecret bool
	caSecretName       string

	inited bool
	err    error
}

// NewCAIniter -
func NewCAIniter(k8sClientsetGetter *K8sClientsetGetter, cmd *cobra.Command) *CAIniter {
	initer := &CAIniter{
		k8sClientsetGetter: k8sClientsetGetter,
	}

	flags := cmd.PersistentFlags()
	flags.BoolVar(
		&initer.preferCAFromSecret,
		"prefer-ca-from-secret",
		true,
		"优先从k8s secret中获取并创建CA路径",
	)
	flags.StringVar(
		&initer.caSecretName,
		"ca-secret-name",
		"ca-secret",
		"从k8s secret中获取ca secret的名字",
	)

	return initer
}

// Init -
func (i *CAIniter) Init() (err error) {
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

	if i.preferCAFromSecret {
		if err := i.initCAFromSecret(cs); err == nil {
			return nil
		}
		fmtError(errors.Wrap(err, "init from secret"))
		fmt.Fprintf(os.Stderr, "====== fallback ======")
	}

	return errors.Wrap(i.initCAFromCache(), "init from cache")
}

func (i *CAIniter) initCAFromSecret(cs *kubernetes.Clientset) error {
	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), i.caSecretName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get secret")
	}

	caPath := filepath.Join(cachePath, defaultCAPath)
	if err := os.MkdirAll(caPath, 0644); err != nil {
		return errors.Wrapf(err, "mkdir all %s", caPath)
	}
	for fileName, content := range secret.Data {
		fullPath := filepath.Join(caPath, fileName)
		if err := os.WriteFile(fullPath, content, 0644); err != nil {
			return errors.Wrapf(err, "write file: %s", fullPath)
		}
	}

	return nil
}

func (i *CAIniter) initCAFromCache() error {
	if _, err := os.Stat(filepath.Join(cachePath, defaultCAPath, "ca.pem")); err != nil {
		return errors.Wrap(err, "stat file")
	}
	if _, err := os.Stat(filepath.Join(cachePath, defaultCAPath, "cert.pem")); err != nil {
		return errors.Wrap(err, "stat file")
	}
	if _, err := os.Stat(filepath.Join(cachePath, defaultCAPath, "key.pem")); err != nil {
		return errors.Wrap(err, "stat file")
	}
	return nil
}

// FixEtcdCfg -
func (i *CAIniter) FixEtcdCfg(cfg *etcd3.ClientConfig) {
	cfg.CaFile = filepath.Join(cachePath, defaultCAPath, "ca.pem")
	cfg.CertFile = filepath.Join(cachePath, defaultCAPath, "cert.pem")
	cfg.KeyFile = filepath.Join(cachePath, defaultCAPath, "key.pem")
}
