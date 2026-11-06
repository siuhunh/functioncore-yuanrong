package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
	_ "unsafe"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	fhttp "github.com/valyala/fasthttp"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/types"
	"functioncore/pkg/common/uuid"
	"functioncore/pkg/worker-manager/config"
	_ "functioncore/pkg/worker-manager/podfetcher"
	_ "functioncore/pkg/worker-manager/server"
	"functioncore/pkg/worker-manager/storage/meta/faasfunction"
	"functioncore/pkg/worker-manager/utils"
)

//go:linkname initFuncRequest functioncore/pkg/worker-manager/podfetcher.initFuncRequest
func initFuncRequest(service *faasfunction.FunctionService,
	reqInfo *config.RequestInfo) types.FunctionSpecRequest

//go:linkname initSpecializeRequest functioncore/pkg/worker-manager/podfetcher.initSpecializeRequest
func initSpecializeRequest(pod *v1.Pod, functionRequest types.FunctionSpecRequest,
	reqInfo *config.RequestInfo, httpsEnable, idleMode bool) (*fhttp.Request, *fhttp.Response, error)

//go:linkname newWorkerClient functioncore/pkg/worker-manager/server.newWorkerClient
func newWorkerClient() *fhttp.Client

// DeployCmd -
type DeployCmd struct {
	k8sClientsetGetter     *K8sClientsetGetter
	workerManagerCfgGetter *WorkerManagerCfgGetter
	etcdCmd                *EtcdCmd
	mockRuntimeCmd         *MockRuntimeCmd
	podName                string
	functionETCDKey        string
	urn                    string
	mockRuntime            bool
	reserved               bool
	snapshot               string
	access                 string
	secret                 string
	secretToken            string
}

// NewDeployCmd -
func NewDeployCmd(
	k8sClientsetGetter *K8sClientsetGetter,
	workerManagerCfgGetter *WorkerManagerCfgGetter,
	etcdCmd *EtcdCmd,
	mockRuntimeCmd *MockRuntimeCmd) *DeployCmd {

	return &DeployCmd{
		k8sClientsetGetter:     k8sClientsetGetter,
		workerManagerCfgGetter: workerManagerCfgGetter,
		etcdCmd:                etcdCmd,
		mockRuntimeCmd:         mockRuntimeCmd,
	}
}

// ApplyCobra -
func (d *DeployCmd) ApplyCobra(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(
		&d.podName,
		"pod-name",
		"",
		"pod名",
	)
	cmd.MarkFlagRequired("pod-name")

	flags.StringVar(
		&d.functionETCDKey,
		"function-etcd-key",
		"",
		"函数etcd key",
	)

	flags.StringVar(
		&d.urn,
		"urn",
		"",
		"函数管理面urn, 当etcd key为空时生效, 示例 urn:fss:cn-north-7:c53626012ba84727b938ca8bf03108ef:function:default:zcy_python_long:latest",
	)

	flags.BoolVar(
		&d.mockRuntime,
		"mock-runtime",
		false,
		"替换使用一个假的rpc runtime, 需要改造worker, 特化时连接30000端口",
	)

	flags.BoolVar(
		&d.reserved,
		"reserved",
		false,
		"是否为预留实例",
	)

	flags.StringVar(
		&d.snapshot,
		"snapshot",
		"",
		"snapshot deployment 配置, json结果 (types.SnapshotDeploymentSpec)",
	)

	flags.StringVarP(
		&d.access,
		"access",
		"i",
		"",
		"凭据轮转 access",
	)

	flags.StringVarP(
		&d.secret,
		"secret",
		"k",
		"",
		"凭据轮转 secret",
	)

	flags.StringVarP(
		&d.secretToken,
		"secret-token",
		"t",
		"",
		"凭据轮转 secret token",
	)

	d.mockRuntimeCmd.ApplyCobra(cmd)
}

// Run -
func (d *DeployCmd) Run() error {
	if d.mockRuntime {
		myExecutable, err := os.Executable()
		if err != nil {
			return errors.Wrap(err, "os executable")
		}
		myExecutable, err = filepath.EvalSymlinks(myExecutable)
		if err != nil {
			return errors.Wrap(err, "eval symlinks")
		}
		base := filepath.Base(myExecutable)
		if err := execCommand("kubectl", "cp", myExecutable, d.podName+":/tmp/"+base, "-c", "runtime"); err != nil {
			return errors.Wrap(err, "kubectl cp")
		}

		if filePath := d.mockRuntimeCmd.invokeResponseFilePath; filePath != "" {
			inContainerFilePath := "/tmp/" + filePath
			if err := execCommand("kubectl", "cp", filePath, d.podName+":"+inContainerFilePath, "-c", "runtime"); err != nil {
				return errors.Wrap(err, "kubectl cp")
			}
			d.mockRuntimeCmd.invokeResponseFilePath = inContainerFilePath
		}

		mockRuntimeArgs := strings.Join(d.mockRuntimeCmd.GenerateArgs(), " ")
		cmd := "/tmp/" + base + " mock-runtime " + mockRuntimeArgs + " &"
		fmt.Println(cmd)
		if err := execCommand("kubectl", "exec", d.podName, "-c", "runtime", "--", "/bin/bash", "-c", cmd); err != nil {
			return errors.Wrap(err, "kubectl exec")
		}
	}

	cs, err := d.k8sClientsetGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get k8s clientset")
	}
	pod, err := cs.CoreV1().Pods("default").Get(context.Background(), d.podName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get pod")
	}

	cfg, err := d.workerManagerCfgGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get worker manager cfg")
	}

	etcdStopCh := make(chan struct{})
	defer close(etcdStopCh)
	if err := d.etcdCmd.InitEtcdClient(etcdStopCh); err != nil {
		return errors.Wrap(err, "init etcd client")
	}

	if d.functionETCDKey == "" {
		etcdKey, err := urnToETCDKey(d.urn)
		if err != nil {
			return errors.Wrap(err, "urn to etcd key")
		}
		d.functionETCDKey = etcdKey
	}

	fmt.Printf("trying to deploy pod %s as %s\n", d.podName, d.functionETCDKey)

	service, _, _, err := utils.GetMetaInfoFromEtcd(d.functionETCDKey)
	if err != nil {
		return errors.Wrap(err, "get function service from etcd")
	}

	reqInfo := utils.PackRequestInfo(service, constants.WorkerManagerApplier, "reserved-"+uuid.New().String())
	if !d.reserved {
		// Assign some other values so that 'applier' is not 'worker-manager'
		// (applier == worker-manager means it is a reserved instance)
		reqInfo.Applier = "dummy"

	}
	if d.snapshot != "" {
		snapshotDeployment := types.SnapshotDeploymentSpec{}
		if err := json.Unmarshal([]byte(d.snapshot), &snapshotDeployment); err != nil {
			return errors.Wrap(err, "json unmarshal")
		}
		fmt.Printf("snapshot: %+v\n", snapshotDeployment)
		reqInfo.SnapshotDeployment = &snapshotDeployment
	}

	functionRequest := initFuncRequest(service, reqInfo)
	if d.access != "" {
		// update token rotation tokens
		functionRequest.Message.Deployment.Access = d.access

		secret, err := localauth.Encrypt(d.secret)
		if err != nil {
			return errors.Wrap(err, "encrypt secret")
		}
		functionRequest.Message.Deployment.Secret = secret

		secretToken, err := localauth.Encrypt(d.secretToken)
		if err != nil {
			return errors.Wrap(err, "encrypt secret token")
		}
		functionRequest.Message.Deployment.SecurityToken = secretToken
	}

	req, response, err := initSpecializeRequest(pod, functionRequest, reqInfo, cfg.HTTPSConfig.HTTPSEnable, false)
	if err != nil {
		return errors.Wrap(err, "init specialize request")
	}
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(response)
	timeout := time.Duration(58)*time.Second + time.Duration(functionRequest.Message.Function.Timeout)*time.Second
	initializerTimeout := functionRequest.Message.Function.Initializer.Timeout
	if initializerTimeout > 0 {
		timeout = timeout + time.Duration(initializerTimeout)*time.Second
	}

	workerClient := newWorkerClient()
	if err := workerClient.DoTimeout(req, response, timeout); err != nil {
		return errors.Wrap(err, "do timeout")
	}

	if response.StatusCode() != http.StatusOK {
		fmt.Println("deploy failed with status code:", response.StatusCode())
	} else {
		fmt.Println("deploy successfully")
	}
	fmt.Println(string(response.Body()))

	return nil
}

func execCommand(name string, args ...string) error {
	cmd := exec.Command(name, args...)
	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout
	if err := cmd.Run(); err != nil {
		return errors.Wrapf(err, "execute cmd: %s, %v", name, args)
	}
	return nil
}

func urnToETCDKey(urn string) (string, error) {
	tokens := strings.Split(urn, ":")
	if len(tokens) != 8 {
		return "", fmt.Errorf("urn '%s' does not contain 8 tokens", urn)
	}
	tenant := tokens[3]
	service := tokens[5]
	funcName := tokens[6]
	version := tokens[7]
	complexFuncName := fmt.Sprintf("0@%s@%s", service, funcName)

	return utils.FunctionKey("yrk", tenant, complexFuncName, version), nil
}
