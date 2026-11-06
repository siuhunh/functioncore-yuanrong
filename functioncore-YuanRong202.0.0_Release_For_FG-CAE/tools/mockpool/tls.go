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
)

const (
	defaultSSLRootPath = "ssl-root"
)

// TLSIniter -
type TLSIniter struct {
	k8sClientsetGetter      *K8sClientsetGetter
	localAuthIniter         *LocalAuthIniter
	preferSSLRootFromSecret bool
	sslRootSecretName       string

	inited bool
	err    error
}

// NewTLSIniter -
func NewTLSIniter(
	k8sClientsetGetter *K8sClientsetGetter,
	localAuthIniter *LocalAuthIniter,
	cmd *cobra.Command) *TLSIniter {

	initer := &TLSIniter{
		k8sClientsetGetter: k8sClientsetGetter,
		localAuthIniter:    localAuthIniter,
	}

	flags := cmd.PersistentFlags()

	flags.BoolVar(
		&initer.preferSSLRootFromSecret,
		"prefer-ssl-root-from-secret",
		true,
		"优先从k8s secret中获取并创建SSL_ROOT路径",
	)
	flags.StringVar(
		&initer.sslRootSecretName,
		"ssl-root-secret-name",
		"https-certs",
		"从k8s secret中获取SSL_ROOT secret的名字",
	)

	return initer
}

// Init -
func (i *TLSIniter) Init() (err error) {
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

	if err := i.localAuthIniter.Init(); err != nil {
		return errors.Wrap(err, "init local auth")
	}

	if i.preferSSLRootFromSecret {
		if err := i.initTLSFromSecret(cs); err == nil {
			return nil
		}
		fmtError(errors.Wrap(err, "init from secret"))
		fmt.Fprintf(os.Stderr, "====== fallback ======")
	}

	return errors.Wrap(i.initTLSFromCache(), "init from cache")
}

func (i *TLSIniter) initTLSFromSecret(cs *kubernetes.Clientset) error {
	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), i.sslRootSecretName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get secret")
	}

	sslRootPath := filepath.Join(cachePath, defaultSSLRootPath)
	if err := os.MkdirAll(sslRootPath, 0644); err != nil {
		return errors.Wrapf(err, "mkdir all %s", sslRootPath)
	}
	for fileName, content := range secret.Data {
		fullPath := filepath.Join(sslRootPath, fileName)
		if err := os.WriteFile(fullPath, content, 0644); err != nil {
			return errors.Wrapf(err, "write file: %s", fullPath)
		}
	}

	return i.initTLS(sslRootPath)
}

func (i *TLSIniter) initTLSFromCache() error {
	sslRootPath := filepath.Join(cachePath, defaultSSLRootPath)
	if _, err := os.Stat(sslRootPath); err != nil {
		return errors.Wrap(err, "stat file")
	}
	return i.initTLS(sslRootPath)
}

func (i *TLSIniter) initTLS(sslRootPath string) error {
	if err := os.Setenv("SSL_ROOT", sslRootPath); err != nil {
		return errors.Wrapf(err, "setenv %s", sslRootPath)
	}

	return nil
}
