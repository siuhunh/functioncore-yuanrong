package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func exitIf(err error) {
	if err != nil {
		fmtError(err)
		os.Exit(1)
	}
}

func fmtError(err error) {
	if err != nil {
		if debug {
			fmt.Fprintf(os.Stderr, "%+v\n", err)
		} else {
			fmt.Fprintf(os.Stderr, "%v\n", err)
		}
	}
}

var (
	debug     bool
	cachePath string
)

func main() {
	rootCmd := &cobra.Command{}
	flags := rootCmd.PersistentFlags()
	flags.StringVar(&cachePath, "cache-path", ".cache", "缓存文件路径")
	flags.BoolVar(&debug, "debug", false, "打印stack-trace")

	k8sClientsetGetter := NewK8sClientsetGetter(rootCmd)
	workerManagerCfgGetter := NewWorkerManagerCfgGetter(k8sClientsetGetter, rootCmd)
	localAuthIniter := NewLocalAuthIniter(k8sClientsetGetter, rootCmd)
	tlsIniter := NewTLSIniter(k8sClientsetGetter, localAuthIniter, rootCmd)
	proxyCfgIniter := NewProxyCfgIniter(k8sClientsetGetter, rootCmd)
	caIniter := NewCAIniter(k8sClientsetGetter, rootCmd)

	startCmd := NewStartCmd(k8sClientsetGetter, tlsIniter)
	mockRuntimeCmd := NewMockRuntimeCmd()
	etcdCmd := NewEtcdCmd(workerManagerCfgGetter, tlsIniter, localAuthIniter, caIniter)
	deployCmd := NewDeployCmd(k8sClientsetGetter, workerManagerCfgGetter, etcdCmd, mockRuntimeCmd)
	pprofCmd := NewPprofCmd(k8sClientsetGetter)
	invokeCmd := NewInvokeCmd(k8sClientsetGetter, tlsIniter, proxyCfgIniter, pprofCmd)
	checkPointCmd := NewCheckpointCmd(k8sClientsetGetter, workerManagerCfgGetter, tlsIniter, localAuthIniter)

	rootCmd.AddCommand(
		CreateCobraCommand("start", "创建pod", startCmd),
		CreateCobraCommand("deploy", "特化pod", deployCmd),
		CreateCobraCommand("pprof", "pprof", pprofCmd),
		CreateCobraCommand("invoke", "调用", invokeCmd),
		CreateCobraCommand("mock-runtime", "mock runtime", mockRuntimeCmd),
		CreateCobraCommand("etcd", "etcd", etcdCmd),
		CreateCobraCommand("checkpoint", "checkpoint", checkPointCmd),
	)

	exitIf(rootCmd.Execute())
}

// CMD -
type CMD interface {
	ApplyCobra(*cobra.Command)
	Run() error
}

// CreateCobraCommand -
func CreateCobraCommand(use, short string, cmd CMD) *cobra.Command {
	cobraCmd := &cobra.Command{
		Use:   use,
		Short: short,
	}
	cmd.ApplyCobra(cobraCmd)
	cobraCmd.RunE = func(*cobra.Command, []string) error { return cmd.Run() }
	return cobraCmd
}

// Keys -
func Keys(m map[string]string) []string {
	x := make([]string, 0, len(m))
	for k := range m {
		x = append(x, k)
	}
	return x
}
