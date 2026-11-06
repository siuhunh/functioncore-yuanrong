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
	defaultLocalAuthCipherPath = "cipher"
)

// LocalAuthIniter -
type LocalAuthIniter struct {
	k8sClientsetGetter *K8sClientsetGetter

	preferCipherFromSecret bool
	cipherSecretName       string

	inited bool
	err    error
}

// NewLocalAuthIniter -
func NewLocalAuthIniter(k8sClientsetGetter *K8sClientsetGetter, cmd *cobra.Command) *LocalAuthIniter {
	initer := &LocalAuthIniter{
		k8sClientsetGetter: k8sClientsetGetter,
	}

	flags := cmd.PersistentFlags()
	flags.BoolVar(
		&initer.preferCipherFromSecret,
		"prefer-cipher-from-secret",
		true,
		"优先从k8s secret获取local-auth cipher",
	)
	flags.StringVar(
		&initer.cipherSecretName,
		"cipher-secret-name",
		"local-secret",
		"k8s local-auth cipher名字",
	)

	return initer
}

// Init -
func (i *LocalAuthIniter) Init() (err error) {
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

	if i.preferCipherFromSecret {
		if err := i.initCipherFromSecret(cs); err == nil {
			return nil
		}
		fmtError(errors.Wrap(err, "init from secret"))
		fmt.Fprintf(os.Stderr, "====== fallback ======")
	}

	return errors.Wrap(i.initCipherFromCache(), "init from cache")
}

func (i *LocalAuthIniter) initCipherFromSecret(cs *kubernetes.Clientset) error {
	secret, err := cs.CoreV1().Secrets("default").Get(context.Background(), i.cipherSecretName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get secret")
	}

	cipherPath := filepath.Join(cachePath, defaultLocalAuthCipherPath)
	if err := os.MkdirAll(cipherPath, 0644); err != nil {
		return errors.Wrapf(err, "mkdir all %s", cipherPath)
	}
	for fileName, content := range secret.Data {
		fullPath := filepath.Join(cipherPath, fileName)
		if err := os.WriteFile(fullPath, content, 0644); err != nil {
			return errors.Wrapf(err, "write file: %s", fullPath)
		}
	}

	return i.initCipher(cipherPath)
}

func (i *LocalAuthIniter) initCipherFromCache() error {
	cipherPath := filepath.Join(cachePath, defaultLocalAuthCipherPath)
	if _, err := os.Stat(cipherPath); err != nil {
		return errors.Wrap(err, "stat file")
	}
	return i.initCipher(cipherPath)
}

func (i *LocalAuthIniter) initCipher(cipherPath string) error {
	if err := os.Setenv("PAAS_CRYPTO_PATH", cipherPath); err != nil {
		return errors.Wrapf(err, "setenv %s", cipherPath)
	}
	return nil
}
