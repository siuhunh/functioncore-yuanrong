package main

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	clientv3 "go.etcd.io/etcd/client/v3"

	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/tls"
)

// EtcdCmd -
type EtcdCmd struct {
	workerManagerCfgGetter *WorkerManagerCfgGetter
	tlsIniter              *TLSIniter
	localAuthIniter        *LocalAuthIniter
	caIniter               *CAIniter
}

// NewEtcdCmd -
func NewEtcdCmd(workerManagerCfgGetter *WorkerManagerCfgGetter,
	tlsIniter *TLSIniter,
	localAuthIniter *LocalAuthIniter,
	caIniter *CAIniter) *EtcdCmd {

	return &EtcdCmd{
		workerManagerCfgGetter: workerManagerCfgGetter,
		tlsIniter:              tlsIniter,
		localAuthIniter:        localAuthIniter,
		caIniter:               caIniter,
	}
}

// ApplyCobra -
func (e *EtcdCmd) ApplyCobra(cmd *cobra.Command) {
}

// Run -
func (e *EtcdCmd) Run() error {
	stopCh := make(chan struct{})
	defer close(stopCh)
	if err := e.InitEtcdClient(stopCh); err != nil {
		return errors.Wrap(err, "init etcd client")
	}

	defaultEtcdCtx := etcd3.CreateEtcdCtxInfoWithTimeout(context.Background(), etcd3.DurationContextTimeout)
	resp, err := etcd3.GetMetaEtcdClient().GetResponse(defaultEtcdCtx, "/sn/functions", clientv3.WithPrefix())
	if err != nil {
		return errors.Wrap(err, "etcd get response")
	}

	for _, v := range resp.Kvs {
		fmt.Println(string(v.Key))
		fmt.Println(string(v.Value))
	}
	return nil
}

// InitEtcdClient -
func (e *EtcdCmd) InitEtcdClient(stopCh <-chan struct{}) error {
	cfg, err := e.workerManagerCfgGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get worker manager cfg")
	}
	enabled := cfg.HTTPSConfig.HTTPSEnable
	if enabled {
		if err := e.tlsIniter.Init(); err != nil {
			return errors.Wrap(err, "init tls")
		}

		if err := tls.InitTLSConfig(cfg.HTTPSConfig.TLSProtocol, cfg.HTTPSConfig.TLSCiphers); err != nil {
			return errors.Wrap(err, "init tls config")
		}
	}

	if err := e.localAuthIniter.Init(); err != nil {
		return errors.Wrap(err, "init local auth")
	}
	if cfg.Etcd.SslEnable {
		if err := e.caIniter.Init(); err != nil {
			return errors.Wrap(err, "init ca")
		}
		e.caIniter.FixEtcdCfg(cfg.Etcd)
	}

	if err = etcd3.InitParam().
		WithStopCh(stopCh).
		WithMetaEtcdConfig(cfg.Etcd).
		InitClient(); err != nil {
		return errors.Wrap(err, "")
	}

	return nil
}
