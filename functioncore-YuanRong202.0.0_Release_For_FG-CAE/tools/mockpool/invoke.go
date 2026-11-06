package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/tls"
	"functioncore/pkg/common/types"
	commonutils "functioncore/pkg/common/utils"
	"functioncore/pkg/functionbus/bus/tcpbus"
	"functioncore/pkg/functionbus/common"
	busutils "functioncore/pkg/functionbus/utils"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/tools/mockpool/analyzer"
)

// InvokeCmd -
type InvokeCmd struct {
	k8sClientsetGetter *K8sClientsetGetter
	tlsIniter          *TLSIniter
	proxyCfgIniter     *ProxyCfgIniter
	pprofCmd           *PProfCmd
	podName            string
	tail               bool
	body               string
	bodyFilePath       string
	concurrency        int
	duration           int
	outputEachResult   bool
	outputErrorResult  bool
	dryRun             bool
	topN               int
	pprof              bool

	// cache
	rawBody []byte
}

// NewInvokeCmd -
func NewInvokeCmd(
	k8sClientsetGetter *K8sClientsetGetter,
	tlsIniter *TLSIniter,
	proxyCfgIniter *ProxyCfgIniter,
	pprofCmd *PProfCmd) *InvokeCmd {

	return &InvokeCmd{
		k8sClientsetGetter: k8sClientsetGetter,
		tlsIniter:          tlsIniter,
		proxyCfgIniter:     proxyCfgIniter,
		pprofCmd:           pprofCmd,
	}
}

// ApplyCobra -
func (i *InvokeCmd) ApplyCobra(cmd *cobra.Command) {
	flags := cmd.Flags()
	flags.StringVar(
		&i.podName,
		"pod-name",
		"",
		"pod名",
	)
	cmd.MarkFlagRequired("pod-name")

	flags.BoolVar(
		&i.tail,
		"tail",
		false,
		"日志LogOption使用Tail",
	)

	flags.StringVar(
		&i.body,
		"body",
		"{}",
		"请求body",
	)

	flags.StringVar(
		&i.bodyFilePath,
		"body-file-path",
		"",
		"请求body文件, 覆盖body",
	)

	flags.IntVar(
		&i.concurrency,
		"concurrency",
		1,
		"请求并发数",
	)

	flags.IntVar(
		&i.duration,
		"duration",
		0,
		"压测执行时常, 如果不填, 只会执行1次请求, 可以使用ctrl-c提前暂停, 单位秒",
	)

	flags.BoolVar(
		&i.outputEachResult,
		"output-each-result",
		false,
		"压测时输出每次请求结果",
	)

	flags.BoolVar(
		&i.outputErrorResult,
		"output-error-result",
		false,
		"压测时输出失败的请求结果",
	)

	flags.BoolVar(
		&i.dryRun,
		"dry-run",
		false,
		"不真正发送请求(用于测试工具执行速度)",
	)

	flags.IntVar(
		&i.topN,
		"top-n",
		10,
		"打印执行时间最长的N个结果",
	)

	flags.BoolVar(
		&i.pprof,
		"pprof",
		false,
		"duration不为0时, 生成pprof, 打开后pprof相关配置才会生效, pprof-seconds的默认值被替换为duration",
	)

	i.pprofCmd.AddExtraFlags(flags)
}

// Run -
func (i *InvokeCmd) Run() error {
	cs, err := i.k8sClientsetGetter.Get()
	if err != nil {
		return errors.Wrap(err, "get k8s clientset")
	}
	pod, err := cs.CoreV1().Pods("default").Get(context.Background(), i.podName, metav1.GetOptions{})
	if err != nil {
		return errors.Wrap(err, "get pod")
	}

	if err := i.tlsIniter.Init(); err != nil {
		return errors.Wrap(err, "init tls")
	}

	if err := i.proxyCfgIniter.Init(); err != nil {
		return errors.Wrap(err, "init proxy cfg")
	}
	if common.FConfig.HTTPSConfig.HTTPSEnable {
		err := tls.InitTLSConfig(common.FConfig.HTTPSConfig.TLSProtocol, common.FConfig.HTTPSConfig.TLSCiphers)
		if err != nil {
			return errors.Wrap(err, "init tls config")
		}
	}

	tcpBusOpt := &tcpbus.Option{CreateClient: true, ServerAddr: pod.Status.PodIP + ":" + utils.P2pPort, Remote: true}
	tcpBus, err := tcpbus.NewTCPBus(common.BusOption{NewBusOpt: tcpBusOpt})
	if err != nil {
		return errors.Wrap(err, "new tcp bus")
	}
	dstProperty := &common.FunctionProperty{
		TenantID:        "",
		FunctionName:    "",
		FunctionVersion: "",
		InstanceID:      commonutils.GenerateInstanceID(pod.Name),
	}

	if i.bodyFilePath != "" {
		content, err := os.ReadFile(i.bodyFilePath)
		if err != nil {
			return errors.Wrap(err, "read file")
		}
		i.rawBody = content
	} else {
		i.rawBody = []byte(i.body)
	}

	analyzer := analyzer.NewResponseAnalyzer(
		i.duration == 0 || i.outputEachResult,
		i.outputErrorResult,
		i.topN,
		i.concurrency)
	flyer := NewFlyer(i.concurrency,
		analyzer.NewLocal,
		func(id int, p interface{}) {
			param := p.(*callParam)
			start := time.Now()
			if !i.dryRun {
				resp, err := tcpBus.Call(param.ctx, param.dst, param.req, false)
				exitIf(err)
				analyzer.AddRecord(id, time.Since(start), param.traceID, newResponse(resp), resp.ErrorCode != 0)
			} else {
				analyzer.AddRecord(id, time.Since(start), param.traceID, nil, false)
			}
		})
	start := time.Now()
	defer func() {
		metrics := analyzer.Calc(time.Since(start))
		fmt.Println(metrics)
	}()

	inputCh, wait := flyer.Run()
	defer wait()
	defer close(inputCh)

	if i.duration == 0 {
		traceID := commonutils.UniqueID()
		i.call(inputCh, dstProperty, traceID)
	} else {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		ctx, cancel := context.WithTimeout(ctx, time.Duration(i.duration)*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		defer wg.Wait()

		if i.pprof {
			wg.Add(1)
			if i.pprofCmd.pprofSeconds == 0 {
				i.pprofCmd.pprofSeconds = i.duration
			}
			i.pprofCmd.podName = i.podName
			go func() {
				exitIf(i.pprofCmd.Run())
				wg.Done()
			}()
		}

		idx := 1
		for {
			select {
			case <-ctx.Done():
				return nil
			default:
			}

			traceID := strconv.Itoa(idx)
			i.call(inputCh, dstProperty, traceID)
			idx++
		}
	}

	return nil
}

const (
	bodyMaxSize = 100
)

func trimToSize(s []byte, size int) []byte {
	if len(s) <= size {
		return s
	}

	res := make([]byte, size+3)
	copy(res, []byte("..."))
	copy(res[3:], s[len(s)-size:])

	return res
}

type response struct {
	resp *types.TCPResponse
}

func newResponse(resp *types.TCPResponse) *response {
	rawData := trimToSize(resp.RawData, bodyMaxSize)
	resp.RawData = rawData

	return &response{resp}
}

// String -
func (resp *response) String() string {
	if resp == nil {
		return ""
	}

	return string(resp.resp.RawData)
}

// GoString -
func (resp *response) GoString() string {
	if resp == nil {
		return ""
	}

	return fmt.Sprintf("%+v", resp.resp)
}

type callParam struct {
	traceID string
	ctx     context.Context
	dst     *common.FunctionProperty
	req     *types.TCPRequest
}

func (i *InvokeCmd) call(inputCh chan<- interface{}, dstProperty *common.FunctionProperty, traceID string) {
	dst := *dstProperty
	dst.FutureID = traceID
	req := &types.TCPRequest{
		RawData:     i.rawBody,
		Concurrency: uint32(i.concurrency),
	}
	if i.tail {
		req.LogType = "Tail"
	}

	inputCh <- &callParam{
		traceID: traceID,
		ctx:     busutils.ContextWithValueString(context.Background(), constants.HeaderTraceID, traceID),
		dst:     &dst,
		req:     req,
	}
}
