/*
 * Copyright (c) 2021 Huawei Technologies Co., Ltd
 *
 * This software is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 * http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

// Package server generic api server
package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/asaskevich/govalidator"
	"github.com/gin-gonic/gin"
	fhttp "github.com/valyala/fasthttp"
	"golang.org/x/net/netutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apiServerOptions "functioncore/cmd/worker-manager/app/options"
	"functioncore/pkg/common/alarm"
	"functioncore/pkg/common/cluster"
	"functioncore/pkg/common/constants"
	"functioncore/pkg/common/etcd3"
	"functioncore/pkg/common/localauth"
	"functioncore/pkg/common/logger/log"
	"functioncore/pkg/common/snapshotmanager"
	"functioncore/pkg/common/tls"
	commonTLS "functioncore/pkg/common/tls"
	"functioncore/pkg/common/tokenrotation"
	"functioncore/pkg/common/urnutils"
	apiV1 "functioncore/pkg/worker-manager/api/v1"
	"functioncore/pkg/worker-manager/cache"
	"functioncore/pkg/worker-manager/checkpoint"
	"functioncore/pkg/worker-manager/config"
	"functioncore/pkg/worker-manager/etcdhandler"
	"functioncore/pkg/worker-manager/eventsource/etcdevents"
	interfacelog "functioncore/pkg/worker-manager/log"
	"functioncore/pkg/worker-manager/monitoring"
	"functioncore/pkg/worker-manager/nodebalance"
	"functioncore/pkg/worker-manager/podfetcher"
	"functioncore/pkg/worker-manager/poolmanager"
	"functioncore/pkg/worker-manager/reactor"
	"functioncore/pkg/worker-manager/state"
	"functioncore/pkg/worker-manager/utils"
	"functioncore/pkg/worker-manager/vpcmanager"
)

const (
	// maxHeaderShiftNum is the max shift numbers of request header
	maxHeaderShiftNum = 20
	// modLockName is the module lock name of worker-manager
	modWorkerManagerLockName = "worker-manager"
	// modLockName is the module lock name of worker-manager
	modSnapshotManagerLockName = "snapshot-manager"
	// HeartbeatsTimeout wait time for health check, three times of which matches for leader election time 16s.
	HeartbeatsTimeout = 5 * time.Second
	// CheckRetryInterval connect leader node time interval
	CheckRetryInterval = 6 * time.Second

	workerClientCallAttempts = 1
	workerClientDialTimeout  = 10 * time.Second
	// worker maximum generalization timeout 33 seconds + function maximum initialization timeout 300 seconds
	workerClientReadTimeout         = 333 * time.Second
	workerClientMaxIdleConnDuration = 5 * time.Second

	maxConnectionConfig    = "MAX_SERVER_CONNECTION"
	defaultConnectionLimit = 5000
)

var (
	onceClient = struct {
		c *fhttp.Client
		sync.Once
	}{}
)

func setupAPIs(options *apiServerOptions.APIServerOptions, apiRegistry *APIRegistry, stopCh <-chan struct{}) error {
	if options.EnableAPIV1 {
		serverAPIV1, err := apiV1.NewAPIV1(options, stopCh)
		if err != nil {
			log.GetLogger().Errorf("creating api v1 with error %s", err.Error())
			return err
		}

		err = apiRegistry.registerAPI(serverAPIV1)
		if err != nil {
			log.GetLogger().Errorf("registering api v1 with error %s", err.Error())
			return err
		}
	}

	return nil
}

func cleanRemainingMigratedPod() {
	pods, err := utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).List(
		context.TODO(), metav1.ListOptions{LabelSelector: utils.LabelMigratedFrom})
	if err != nil {
		log.GetLogger().Errorf("failed to list migrate pod, err: %s", err.Error())
		return
	}
	for i := range pods.Items {
		if len(pods.Items[i].OwnerReferences) == 0 {
			log.GetLogger().Infof("start to clean remaining migratedPod %s", pods.Items[i].Name)
			utils.DeletePod(&pods.Items[i])
		}
	}
}

func cleanRemainingTMPExpansionPod() {
	pods, err := utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).List(
		context.TODO(), metav1.ListOptions{LabelSelector: utils.LabelEnableTMPExpansion + "=" + utils.TrueString})
	if err != nil {
		log.GetLogger().Errorf("failed to list tmp expansion pod, err: %s", err.Error())
		return
	}
	for i := range pods.Items {
		if len(pods.Items[i].OwnerReferences) == 0 {
			log.GetLogger().Infof("start to clean remaining tmp expansion %s", pods.Items[i].Name)
			utils.DeletePod(&pods.Items[i])
		}
	}
}

func initAllModules() error {
	start := time.Now()
	etcd3.GetRouterEtcdClient().RegisterRedoFunc(etcdhandler.OperateETCD, etcd3.RedoFailedOperations)
	registerAlarmTypes()
	if err := monitoring.InitMetricsCache(); err != nil {
		log.GetLogger().Errorf("failed to init metrics cache, %s", err.Error())
		return err
	}
	if err := utils.InitWorkerManagerDeploy(); err != nil {
		log.GetLogger().Errorf("failed to init worker manager deploy: %s", err.Error())
		return err
	}
	cleanRemainingMigratedPod()
	cleanRemainingTMPExpansionPod()
	listStart := time.Now()
	instancePodList, err := utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).List(
		context.TODO(), metav1.ListOptions{LabelSelector: "isPoolPod=false"})
	if err != nil {
		log.GetLogger().Errorf("failed to list all none-pool pod with error: %s", err.Error())
		return err
	}
	log.GetLogger().Infof("finished to run the list pods with label isPoolPod=false cost time %s",
		time.Since(listStart).String())
	log.GetLogger().Infof("there are %d instance pods", len(instancePodList.Items))
	poolmanager.GetPoolManager().InitTerminatingLongRun(instancePodList.Items)
	pullTriggerPods, err := utils.GetKubernetesClient().CoreV1().Pods(utils.FunctionNameSpace()).List(
		context.TODO(), metav1.ListOptions{LabelSelector: "cff-type=serverless"})
	if err != nil {
		log.GetLogger().Errorf("failed to list pull trigger pods with error %s", err.Error())
		return err
	}
	log.GetLogger().Infof("there are %d pull trigger pods", len(pullTriggerPods.Items))
	// init worker cache when becoming leader
	if err = initAllCache(instancePodList.Items); err != nil {
		return err
	}

	vpcmanager.DeleteRemainedVpcPullTrigger()
	if err = vpcmanager.InitVPCManager(instancePodList.Items, pullTriggerPods.Items); err != nil {
		log.GetLogger().Errorf("failed to init vpc manager with error %s", err.Error())
		return err
	}
	log.GetLogger().Infof("finished to run the initAllModules cost time %s",
		time.Since(start).String())
	return nil
}

func initAllCache(instancePods []corev1.Pod) error {
	if err := cache.InitStateWorkersFromETCD(); err != nil {
		log.GetLogger().Errorf("failed to init state workers from meta etcd: %s", err.Error())
		return err
	}
	if err := cache.InitWorkerCache(instancePods); err != nil {
		log.GetLogger().Errorf("failed to init worker cache: %s", err.Error())
		return err
	}
	if err := cache.InitNodeCache(instancePods); err != nil {
		log.GetLogger().Errorf("failed to init node cache: %s", err.Error())
		return err
	}
	cache.HasSynced = true
	return nil
}

func initStateCache() {
	podActions := cache.InitStatefulCache()
	if len(podActions) == 0 {
		return
	}
	if p := podfetcher.GetPodFetcher(); p == nil {
		log.GetLogger().Errorf("podfetcher is nil")
		return
	}
	for _, podAction := range podActions {
		podfetcher.GetPodFetcher().DoActions(podAction.FuncSvc, podAction.StatefulAction, podAction.StatefulMeta)
	}
}

func registerAlarmTypes() {
	alarm.GetClient().RegisterAlarmType(alarm.EtcdOperationFailedContinuously)
	alarm.GetClient().RegisterAlarmType(alarm.InsufficientEphemeralStorage)
	alarm.GetClient().RegisterAlarmType(alarm.CheckpointAbnormalContinuously)
	alarm.GetClient().RegisterAlarmType(alarm.CustomImageSchedulerFailed)
}

func startWatch(stopCh <-chan struct{}) error {
	start := time.Now()
	// add callback to process config update when becoming leader
	config.AddConfigChangedCallback(processConfigUpdate)
	config.AddConfigChangedCallback(processDecoyConfigUpdate)
	config.AddConfigChangedCallback(processEnableMultiSnapshotUpdate)
	config.AddConfigChangedCallback(processCheckpointConfigUpdate)
	config.AddConfigChangedCallback(cache.UpdateImageCacheEnableStatus)
	if err := tokenrotation.StartTokenRotation(stopCh); err != nil {
		return err
	}

	// initializes the client for the first time,if the initialization fails, the worker-manager server exits
	kubeClient := utils.GetKubernetesClient()
	if kubeClient == nil {
		log.GetLogger().Errorf("get k8s client error")
		return errors.New("get Kubernetes client error")
	}
	// init all modules about deploy, worker cache, vpc manager
	if err := initAllModules(); err != nil {
		log.GetLogger().Errorf("failed init all modules with error %s", err.Error())
		return err
	}

	if etcd3.GetCaeRouterEtcdClient() != nil {
		if err := etcdhandler.OperateCAEETCDWithRetry(context.Background(), utils.InitInstancePrefix, "",
			etcdhandler.DelOperation); err != nil {
			log.GetLogger().Warnf("failed to clear init instance in cae router etcd %s", err.Error())
		}
	}

	funcActions := cache.WorkerCache.GenerateInitRollingCAEFuncActions()

	go cache.UpdateAllTenantCache(stopCh, utils.UpdateTenantCacheInterval)
	go cache.UpdateAllFuncDNSDomain(stopCh, utils.UpdateDomainNamesInterval)
	go podfetcher.UpdateAllFuncAgency(stopCh, utils.UpdateAgencyInterval)
	k8sReactor, metaReactor, err := reactor.NewEventDrive("faas", kubeClient, stopCh)
	if err != nil {
		log.GetLogger().Errorf("error: %v\n", err)
		return err
	}

	if err = checkpoint.InitManager(newWorkerClient(), stopCh); err != nil {
		log.GetLogger().Errorf("failed to init checkpoint manager: %s", err.Error())
		return err
	}

	monitoring.ReportMetricsToServer(stopCh)
	if err = cache.RunImageManager(stopCh); err != nil {
		log.GetLogger().Errorf("failed to run image manager: %s", err.Error())
		return err
	}

	poolmanager.Run(stopCh)
	k8sReactor.Run(stopCh)
	metaReactor.Run(stopCh)
	cache.CheckMinInsAndReport(stopCh)

	cache.RunDefaultResyncer(stopCh)

	nodebalance.Run(stopCh, poolmanager.MigratePodToSpecificNode, poolmanager.GetScoreByNodeAndProfiling)
	etcdhandler.RegisterToCAERouterETCD(stopCh)
	initStateCache()
	for i := range funcActions {
		fa := funcActions[i]
		go podfetcher.GetPodFetcher().DoActions(fa.FuncSvcID, fa.Actions, fa.Metadata)
	}
	log.GetLogger().Infof("finished to run the callback when becoming leader with cost time %s",
		time.Since(start).String())
	return nil
}

func createClusterDiscovery(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{},
	wg *sync.WaitGroup) (cluster.Discovery, error) {
	if options == nil {
		return nil, errors.New("conf is nil when create clusterDiscovery")
	}
	if !options.IsClusterMode {
		return cluster.NewSingleNodeCluster(options.ListenAddress, stopCh, wg, log.GetLogger())
	}

	lockName := modWorkerManagerLockName
	if options.IsSnapshotMode {
		lockName = modSnapshotManagerLockName
	}
	// default use lease resource lock for leader elect
	if config.EnableHTTPS() {
		tlsConfig := commonTLS.GetClientTLSConfig()
		return cluster.NewResourceLockCluster(options.ListenAddress, stopCh, lockName, tlsConfig)
	}
	return cluster.NewResourceLockCluster(options.ListenAddress, stopCh, lockName, nil)
}

func newWorkerClient() *fhttp.Client {
	cli := &fhttp.Client{
		ReadTimeout:               workerClientReadTimeout,
		MaxIdleConnDuration:       workerClientMaxIdleConnDuration,
		MaxIdemponentCallAttempts: workerClientCallAttempts,
		Dial: func(addr string) (conn net.Conn, e error) {
			return fhttp.DialTimeout(addr, workerClientDialTimeout)
		},
	}
	if config.EnableHTTPS() {
		newCfg := *commonTLS.GetClientTLSConfig()
		newCfg.NextProtos = []string{"http/1.1"}
		cli.TLSConfig = &newCfg
	}
	return cli
}

// executeSnapshotModeProcess executeSnapshotModeProcess
func executeSnapshotModeProcess(stopCh <-chan struct{}, c *config.FaasConfig) error {
	snapshotmanager.SetSnapshotManagerMode(true)
	// init pod informer and node informer
	log.GetLogger().Infof("execute snapshotModeProcess")
	utils.InitInformer(stopCh)
	workerClient := newWorkerClient()

	if err := config.InitEnvs(); err != nil {
		return err
	}

	if err := alarm.NewClient(c.AlarmServerAddress, constants.SnapshotManager, stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth(c.LocalAuth.AKey, c.LocalAuth.SKey)); err != nil {
		log.GetLogger().Errorf("failed to init alarm client, error: %s", err.Error())
		return err
	}

	if err := poolmanager.CreatePoolMgr(workerClient, stopCh); err != nil {
		log.GetLogger().Errorf("failed to build a pool manager, error: %s", err.Error())
		return err
	}
	if err := utils.InitETCDClient(stopCh); err != nil {
		log.GetLogger().Errorf("init etcd client error, msg: %s\n", err.Error())
		return err
	}
	cache.RegisterRouteEtcdRestartCallback()
	if err := podfetcher.CreatePodFetcher(workerClient, stopCh); err != nil {
		log.GetLogger().Errorf("failed to build a pod fetcher, error: %s", err.Error())
		return err
	}
	err := monitoring.CreateMonitoringClient(c.MonitoringServerConfig, c.PoolMgrConfig.PoolRecordingCFG, stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to create metrics client, error: %s", err.Error())
		return err
	}

	etcdevents.RegisterReadyChan()

	err = initExecution(stopCh)
	if err != nil {
		return err
	}

	log.GetLogger().Infof("succeed to execute SnapshotModeProcess")
	return nil
}

func initExecution(stopCh <-chan struct{}) error {
	if err := tokenrotation.StartTokenRotation(stopCh); err != nil {
		return err
	}
	// initializes the client for the first time,if the initialization fails, the worker-manager server exits
	kubeClient := utils.GetKubernetesClient()
	if kubeClient == nil {
		log.GetLogger().Errorf("get k8s client error")
		return errors.New("get Kubernetes client error")
	}
	// init all modules about deploy, worker cache, vpc manager
	if err := initAllModules(); err != nil {
		log.GetLogger().Errorf("failed init all modules with error %s", err.Error())
		return err
	}

	k8sReactor, metaReactor, err := reactor.NewEventDrive("snapshot", kubeClient, stopCh)
	if err != nil {
		log.GetLogger().Errorf("error: %v", err)
		return err
	}

	poolmanager.Run(stopCh)
	k8sReactor.Run(stopCh)
	metaReactor.Run(stopCh)

	if err := checkpoint.InitManager(newWorkerClient(), stopCh); err != nil {
		log.GetLogger().Errorf("failed to init checkpoint manager: %s", err.Error())
		return err
	}
	return nil
}

// getHandler get handler
func getHandler(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}, c *config.FaasConfig) (
	*APIServerHandler, *sync.WaitGroup, error) {
	var wg sync.WaitGroup
	// init pod informer and node informer
	utils.InitInformer(stopCh)
	workerClient := newWorkerClient()

	if err := config.InitEnvs(); err != nil {
		return nil, &wg, err
	}

	if err := alarm.NewClient(c.AlarmServerAddress, constants.InstanceManager, stopCh,
		alarm.WithTLSEnable(tls.GetClientTLSConfig()), alarm.WithAuth(c.LocalAuth.AKey, c.LocalAuth.SKey)); err != nil {
		log.GetLogger().Errorf("failed to init alarm client, error: %s", err.Error())
		return nil, &wg, err
	}

	if err := poolmanager.CreatePoolMgr(workerClient, stopCh); err != nil {
		log.GetLogger().Errorf("failed to build a pool manager, error: %s", err.Error())
		return nil, &wg, err
	}
	if err := utils.InitETCDClient(stopCh); err != nil {
		fmt.Printf("init etcd client error, msg: %s\n", err.Error())
		return nil, &wg, err
	}
	cache.RegisterRouteEtcdRestartCallback()
	if err := podfetcher.CreatePodFetcher(workerClient, stopCh); err != nil {
		log.GetLogger().Errorf("failed to build a pod fetcher, error: %s", err.Error())
		return nil, &wg, err
	}
	err := monitoring.CreateMonitoringClient(c.MonitoringServerConfig, c.PoolMgrConfig.PoolRecordingCFG, stopCh)
	if err != nil {
		log.GetLogger().Errorf("failed to create metrics client, error: %s", err.Error())
		return nil, &wg, err
	}

	clusterDiscovery, err := createClusterDiscovery(options, stopCh, &wg)
	if err != nil {
		return nil, &wg, err
	}

	etcdevents.RegisterReadyChan()
	clusterDiscovery.SetCallback(startWatch)
	cluster.RegisterOnStoppedLeadingCallback(onStoppedLeadingCallback)
	if err = clusterDiscovery.Discovery(); err != nil {
		log.GetLogger().Errorf("failed to do api server clusterDiscovery, error: %s", err.Error())
		return nil, &wg, err
	}

	clusterCfg, err := cluster.GetResourceLockConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get cluster resource lock config, error: %s", err.Error())
		return nil, &wg, err
	}
	waitTime := time.Duration(clusterCfg.RenewDeadline+clusterCfg.RetryPeriod+1) * time.Second
	go checkHeartbeats(clusterDiscovery, waitTime)
	apiRegistry := newAPIRegistry()
	if err = setupAPIs(options, apiRegistry, stopCh); err != nil {
		log.GetLogger().Errorf("failed to setup APIs of APIServer , error: %s", err.Error())
		return nil, &wg, err
	}

	handler, err := newAPIServerHandler()
	if err != nil {
		log.GetLogger().Errorf("failed to create a new APIServer handler, error: %s", err.Error())
		return nil, &wg, err
	}
	handler.Discovery = clusterDiscovery
	handler.Registry = apiRegistry
	handler.AuthCheck = c.AuthenticationEnable
	handler.LocalAuth = c.LocalAuth
	handler.ForwardRequest = c.EtcdCluser.ForwardRequest

	apiRegistry.Router.GET("/role", func(c *gin.Context) {
		handler.getRole(c.Writer, c.Request)
	})
	apiRegistry.Router.GET("/leader", func(c *gin.Context) {
		handler.getLeader(c.Writer, c.Request)
	})
	return handler, &wg, nil
}

func onStoppedLeadingCallback() {
	log.GetLogger().Infof("worker-manager role has turned to follower, set cache.HasSynced to false")
	cache.HasSynced = false
}

// Run run api server and setup apis
func Run(options *apiServerOptions.APIServerOptions, stopCh <-chan struct{}) error {
	start := time.Now()
	if _, err := govalidator.ValidateStruct(*options); err != nil {
		fmt.Printf("failed to validate option: %s\n", err.Error())
		return err
	}

	// set config file path
	if err := interfacelog.CreateInterLogger(options.LogPath); err != nil {
		fmt.Printf("failed to create inter logger: %s\n", err.Error())
		return err
	}
	log.GetLogger().Infof("begin to run api server with listen address %s", options.ListenAddress)

	config.InitConfigPath(options.ConfigFile)
	cfg, err := config.GetConfig()
	if err != nil {
		fmt.Printf("failed to get config: %s\n", err.Error())
		return err
	}
	utils.SetSmartWarmerEnable(cfg.SmartWarmerEnable)
	utils.SetUpdateFunctionCacheEnable(cfg.UpdateFunctionCacheEnable)
	utils.SetMultiSnapshotConfig(cfg.PoolMgrConfig.MultiSnapshotConfig)
	utils.SetSnapshotTenantIDsConfig(cfg.PoolMgrConfig.SnapshotTenantIDsConfig)
	utils.StartClearKmsCipherCacheTask(stopCh)

	err = state.InitHTTPClient()
	if err != nil {
		log.GetLogger().Errorf("failed to create http client: %s", err.Error())
		return err
	}
	urnutils.SetSeparator(cfg.FunctionNameSeparator)

	// init config watcher, which failed do not return, just config hot load not enable
	// all need sync config update, but only leader need to process
	if err := config.WatchConfig(stopCh, syncConfig); err != nil {
		log.GetLogger().Errorf("failed to start config watcher, %s", err)
	}

	if options.IsSnapshotMode {
		err := executeSnapshotModeProcess(stopCh, cfg)
		if err != nil {
			fmt.Printf("failed to execute snapshotMode process error: %s\n", err.Error())
			return err
		}
		return nil
	}

	handler, wg, err := getHandler(options, stopCh, cfg)
	if err != nil {
		fmt.Printf("run api server with get handler error: %s\n", err.Error())
		return err
	}

	if err := startHTTPServer(options, handler, wg, stopCh); err != nil {
		fmt.Printf("failed to run server: %s\n", err.Error())
		return err
	}
	log.GetLogger().Infof("finished to start api server with cost time %s", time.Since(start).String())
	return nil
}

func getMaxServerConnection() int {
	envConfig := os.Getenv(maxConnectionConfig)
	if envConfig == "" {
		return defaultConnectionLimit
	}
	c, err := strconv.Atoi(envConfig)
	if err != nil {
		return defaultConnectionLimit
	}
	return c
}

func startHealthCheckServer() {
	podIP := os.Getenv(constants.PodIPEnvKey)
	if net.ParseIP(podIP) == nil {
		fmt.Printf("failed to get pod ip of worker-manager from environment")
		return
	}
	router := gin.New()
	router.GET("/healthz", func(c *gin.Context) {
		return
	})
	server := &http.Server{
		Addr:           podIP + ":58867",
		Handler:        router,
		MaxHeaderBytes: 1 << maxHeaderShiftNum,
	}
	if config.EnableHTTPS() {
		server.TLSConfig = commonTLS.GetClientTLSConfig()

		fmt.Printf("health check https server listening on %s\n", server.Addr)
		if err := server.ListenAndServeTLS("", ""); err != nil {
			fmt.Printf("https server has exited: %s\n", err.Error())
		}
		return
	}

	fmt.Printf("health check http server listening on %s\n", server.Addr)
	if err := server.ListenAndServe(); err != nil {
		fmt.Printf("http server has exited: %s\n", err.Error())
		return
	}
}

func startHTTPServer(options *apiServerOptions.APIServerOptions, handler http.Handler, wg *sync.WaitGroup,
	stopCh <-chan struct{}) error {
	if !options.IsSnapshotMode {
		go startHealthCheckServer()
	}
	// max http header is 2M(not include body)
	server := &http.Server{
		Addr:           options.ListenAddress,
		Handler:        handler,
		MaxHeaderBytes: 1 << maxHeaderShiftNum,
	}

	protocol := options.ProtocolType
	log.GetLogger().Infof("begin to listen server with %s protocol", protocol)

	// When received the signal of os in {SIGINT, SIGTERM} will shutdown the main goroutine
	go waitUtilInterrupt(stopCh, wg, server)

	ln, err := net.Listen("tcp", server.Addr)
	if err != nil {
		log.GetLogger().Errorf("failed to start listen, err: %s", err.Error())
		return err
	}
	defer ln.Close()

	l := netutil.LimitListener(ln, getMaxServerConnection())

	if config.EnableHTTPS() {
		server.TLSConfig = commonTLS.GetClientTLSConfig()

		fmt.Printf("https api server listening on %s\n", server.Addr)
		if err := server.ServeTLS(l, "", ""); err != nil {
			fmt.Printf("https server has exited: %s\n", err.Error())
			return err
		}
		return nil
	}

	fmt.Printf("http api server listening on %s\n", server.Addr)
	if err := server.Serve(l); err != nil {
		fmt.Printf("http server has exited: %s\n", err.Error())
		return err
	}
	return nil
}

func waitUtilInterrupt(stopCh <-chan struct{}, wg *sync.WaitGroup, server *http.Server) {
	if stopCh == nil {
		log.GetLogger().Errorf("waitUtilInterrupt's stopCh is nil")
		return
	}
	select {
	case _, ok := <-stopCh:
		if !ok {
			log.GetLogger().Warnf("stopCh closed")
		}
		// wait all groutine graceful quit
		wg.Wait()
		// We received an interrupt signal, shutdown main goroutine.
		if err := server.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout:
			log.GetLogger().Errorf("http server shutdown error: %v", err)
		}
	}
}

// PrintInputLog prints health log
func PrintInputLog() {
	poolManager := poolmanager.GetPoolManager()
	if poolManager == nil {
		log.GetLogger().Warnf("worker-manager is alive. The poolManager is nil")
		return
	}
	poolNum := poolManager.GetPoolNum()
	funcWorkerNum := cache.WorkerCache.GetWorkerRecords()
	log.GetLogger().Infof("worker-manager is alive. The number of pool workers is %d and the number of"+
		" function workers is %d", poolNum, funcWorkerNum)
}

// checkHeartbeats ticker to check cluster heartbeats
func checkHeartbeats(clusterDiscovery cluster.Discovery, waitTime time.Duration) {
	time.Sleep(waitTime)
	connectTicker := time.NewTicker(CheckRetryInterval)
	var failedTimes int
	for {
		select {
		case <-connectTicker.C:
			if clusterDiscovery.Role() == cluster.ClusterRoleLeader {
				failedTimes = 0
				log.GetLogger().Infof("The node switched to leader,with no need to check.")
				return
			}
			connected := listenBackNode(clusterDiscovery, waitTime)
			if !connected {
				failedTimes++
				log.GetLogger().Warnf("The connect failed %d times.", failedTimes)
			} else {
				failedTimes = 0
			}
		}
	}
}

// listenBackNode listen health every period
func listenBackNode(clusterDiscovery cluster.Discovery, waitTime time.Duration) bool {
	leaderIP := clusterDiscovery.Leader().Host
	cfg, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get node config, err %s", err.Error())
		return false
	}
	authorization, timestamp := localauth.CreateAuthorization(cfg.LocalAuth.AKey, cfg.LocalAuth.SKey,
		"", "auth-cli", nil)
	req := fhttp.AcquireRequest()
	resp := fhttp.AcquireResponse()
	defer fhttp.ReleaseRequest(req)
	defer fhttp.ReleaseResponse(resp)
	req.Header.SetMethod(http.MethodGet)
	req.Header.Set(constants.HeaderAuthorization, authorization)
	req.Header.Set(constants.HeaderAuthTimestamp, timestamp)
	req.Header.ResetConnectionClose()
	req.SetRequestURI("/componenthealth")
	req.Header.SetHost(fmt.Sprintf("%s", leaderIP))
	if cfg.HTTPSConfig.HTTPSEnable {
		req.URI().SetScheme("https")
	}
	client := GetClient()
	err = client.DoTimeout(req, resp, waitTime)
	if err != nil {
		log.GetLogger().Warnf("failed to request leader instance %s, err %s", req.URI().String(), err.Error())
		return false
	}
	if resp != nil && resp.StatusCode() == http.StatusOK {
		log.GetLogger().Debugf("connect leader node success")
		return true
	}
	if resp != nil && resp.StatusCode() != http.StatusOK {
		log.GetLogger().Warnf("failed to request leader instance ,states code: %d",
			resp.StatusCode())
		return false
	}
	return false
}

// GetClient init a client
func GetClient() *fhttp.Client {
	onceClient.Do(func() {
		onceClient.c = &fhttp.Client{ReadTimeout: HeartbeatsTimeout}
		if config.EnableHTTPS() {
			newCfg := *commonTLS.GetClientTLSConfig()
			newCfg.NextProtos = []string{"http/1.1"}
			onceClient.c.TLSConfig = &newCfg
		}
	})
	return onceClient.c
}

func syncConfig() {
	newConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get new config: %s", err.Error())
		return
	}
	if poolmanager.GetPoolManager() == nil {
		log.GetLogger().Warnf("failed to sync config to poolMgr when poolMgr is nil")
		return
	}
	// sync config to poolMgr
	if err := poolmanager.GetPoolManager().UpdatePoolManagerConfig(newConfig); err != nil {
		log.GetLogger().Errorf("failed to sync new config to poolMgr: %s", err.Error())
		return
	}
	utils.SetSmartWarmerEnable(newConfig.SmartWarmerEnable)
	log.GetLogger().Infof("succeed to sync worker-manager config update")
}

func processConfigUpdate() {
	poolmanager.GetPoolManager().ProcessPoolSizeUpdate()
	poolmanager.GetPoolManager().ProcessPoolScaleLimitUpdate()
	poolmanager.GetPoolManager().ProcessPoolProactiveModeUpdate()
	utils.UpdateAllPrewarmPod()
	log.GetLogger().Infof("finished to process worker-manager config update")
}

func processEnableMultiSnapshotUpdate() {
	checkpoint.GetCheckpointManager().ProcessMultiSnapshotEnableUpdate()
	log.GetLogger().Infof("finished to process worker-manager multiSnapshotEnable update")
}

func processCheckpointConfigUpdate() {
	newConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get new config: %s", err.Error())
		return
	}
	// sync config to checkpointMgr
	checkpoint.GetCheckpointManager().UpdateCheckpointPodConfig(newConfig)
	log.GetLogger().Infof("finished to process worker-manager checkpointConfig update")
}

func processDecoyConfigUpdate() {
	newConfig, err := config.GetConfig()
	if err != nil {
		log.GetLogger().Errorf("failed to get new config: %s", err.Error())
		return
	}
	poolmanager.GetPoolManager().RestorePool(newConfig)
	log.GetLogger().Infof("finished to process worker-manager DecoyConfigEnable update")
}
