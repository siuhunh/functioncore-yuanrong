# BridgeManager作为插件集成到State-Manager中

### 1.1 功能描述

BridgeManager作为管理BridgeFunc组件，以插件的形式存在于state-manager之中，监听、读取元数据etcd中BridgeFunc的相关信息，并维护client函数实例、stateid、BridgeFunc函数实例之间的关系，在bridgefunc从0-1扩容的时候更新关系，在functiontask发送getBridgeFunc请求时候决策stateKey的分配，在发送release请求时候决策BridgeFunc函数的缩容。



### 1.2 涉及需求

【serviceBridge】监听Bridge函数的异常，释放Bridge资源：AR.SR.IREQ2ecc3fc2.002.004

【serviceBridge】BridgeManager插件支持BridgeFunc的管理和扩缩容：AR.SR.IREQ2ecc3fc2.002.003

【serviceBridge】支持从etcd读取、监听BridgeFunc的配置和更新：AR.SR.IREQ2ecc3fc2.002.002

【serviceBridge】支持Bridge函数的创建：AR.SR.IREQ2ecc3fc2.002.001



### 2. 场景分析与设计

#### 2.1 getBridgeFunc

![image-20221108104206140](http://image.huawei.com/tiny-lts/v1/images/0dc0f896df286948765a716d4599bc7e_1104x740.png)



​		BridgeManager作为插件跟随state-manager一同启动并进行初始化，初始化中会从ETCD中读取持久化的数据并加载到自身的缓存中，如果是首次启动，etcd没有相关数据，则做默认的初始化。同时还会监听etcd函数元数据，如果是BridgeFunction，就会读取元数据的配置，计算允许分配的stateKey的最大上限。

​		当functiontask发送getBridgeFunc的请求时，BridgeManager进行分配stateKey

 	1、如果用户未指定stateID，优先从已有的stateKey中选择引用计数未达到该函数配置的maxActive的
 	2、如果用户未指定stateID，同时当前已有stateKey引用计数都已经达到maxActive，且stateKey数量没有达到上限，则生成新的stateKey，如果达到上限，则返回错误信息
 	3、如果用户指定stateID，则返回空给functiontask，functiontask会自己返回存着的对应的stateKey也就是instanceName

​		分配完成后将对应stateId的相关信息持久化到etcd中

​		之后functiontask拿到返回的stateKey负责创建状态路由



#### 2.2 invoke扩容

![image-20221108114502960](http://image.huawei.com/tiny-lts/v1/images/b1f8c971aa945454918175321a9e0f4e_1493x989.png)



​		functiontask会根据stateid对应实例的情况决策是否进行BridgeFunc函数实例的扩容，若需要扩容，则向worker-manager发送扩容请求，BridgeManager监听etcd中实例的变化信息，当感知对应stateid的实例创建时，更新补充BridgeFunc函数实例信息，更新本地缓存保持的关系，同时将对应stateId的信息持久化到etcd



#### 2.3 release缩容

![image-20221108114728017](http://image.huawei.com/tiny-lts/v1/images/d6d713fb37a0684e323021fb6b9893c2_1478x985.png)



1、BridgeManager插件接收由functiontask发出的release请求，请求中包含client的函数实例信息
2、worker-manager感知client函数实例异常退出时，会清理路由etcd中相关实例信息，BridgeManager插件监听并更新维护关系
3、BridgeManager插件感知client函数实例全部退出时，向请求返回信息中加入缩容内容，functiontask调用terminate流程，缩容实例



#### 2.4 Client函数实例异常

​		worker-manager感知client函数实例异常退出时，会清理路由etcd中相关实例信息，BridgeManager插件监听到对应事件会在缓存中查找这个client是否关联bridgefunc，是的话，清理与之相关的关系



#### 2.5 BridgeFunc函数实例异常

​		worker-manager感知BridgeFunc函数实例异常后，再次有请求调度时，functiontask会向worker-manager重新申请新的实例，worker-manager重新接收扩容请求时，将重新拉起的BridgeFunc实例，同时将原有的stateid绑定至新的实例上，此时BridgeManager插件监听到此事件，重新更新对应关系并持久化，因此不需要监听bridgefunc实例清除的这一事件



### 3 详细方案以及修改点

##### 3.1 数据结构

公共部分

```go
type ClienInstance struct {
	PodName       string          `json:"podName"`
	AssignStateId string          `json:"assignStateId,omitempty"`
	FuncInfo      *BridgeFuncInfo `json:"bridgeFuncInfo"`
}

type BridgeFuncInfo struct {
	TenantId       string `json:"tenantId"`
	ServiceId      string `json:"serviceId"`
	BridgeFuncName string `json:"bridgeFuncName"`
}

type BridgeInstance struct {
	PodName       string
	BridgeFuncUrn string
}
```



bridgecache(单例结构)

```go
type bridgeCache struct {
	// ClientPodClientInstCache key is userFunc podName + tenantId + bridgeFuncName, value is clientInstance
	ClientPodClientInstCache sync.Map

	// FuncNameInfoCache key is bridge function name, value is *bridgeFuncRestrict
	FuncNameInfoCache sync.Map

	// StateIDInfoCache key is StateID, value is StateIDInfo object
	StateIDInfoCache sync.Map

	// StateIDLockMap key is stateID, value is *sync.Mutex lock
	StateIDLockMap sync.Map
}

type StateIDInfo struct {
	StateKey string `json:"stateKey"`
	// PodName is bridgefunc podName
	PodName       string `json:"bridgePodName"`
	RefCount      int    `json:"refCount"`
	BridgeFuncURN string `json:"bridgeFuncURN,omitempty"`
	MaxBgInstQua  int    `json:"maxBgInstQua,omitempty"`
	MaxActiveQua  int    `json:"maxActiveQua,omitempty"`
}

type bridgeFuncRestrict struct {
	maxActive      int
	maxStateKeyQua int
}
```

##### 3.2 持久化路径和结构

- **/sn/bridgemanager/business/yrk/tenant/${tenantId}/function/${complexFuncName}/version/latest/defaultaz/${stateId}**

  用于持久化 stateIdInfoCache

  内容样例：

  {"stateKey":"xxx","bridgePodName":"poolxxxx","refCount":1,"bridgeFuncUrn":"xxxx","maxBgInstQua":20,"maxActiveQua":20}

  

- **/sn/bridgeclient/business/yrk/tenant/${tenantId}/function/${complexFuncName}/version/latest/defaultaz/defaultaz-#-${podName}**

  用于持久化 clientPodClientInstCache

  内容样例：

  {

  ​	"podName":"pool29-1600-1792-fusion-7c4db655b5-zk28x",

  ​	"assignStateId":"264fd22e-d336-5de2-980a-121e7895ae0c",

  ​	"bridgeFuncInfo":{

  ​		"tenantId":"7e1ad6a6-cc5c-44fa-bd54-25873f72a86a",

  ​		"serviceId":"0@base",

  ​		"bridgeFuncName":"rds1108"

  ​	}

  }

- 



##### 3.3 持久化时机

- ApplyStateKey

  分配stateKey或者用户指定stateKey后，根据最新的关系，持久化 /sn/bridgemanager 和 /sn/bridgeclient

- NewBridge

  更新stateIdInfo中的podName，持久化  /sn/bridgemanager

- ReleaseBridge

  - 如果引用计数未归零，不需要释放，此时会更新stateIdInfo中的refCount，持久化/sn/bridgemanager
  - 如果引用计数归零，删除stateIdInfo中对应stateId的记录，同时删除对应的持久化数据
  - 无论是否归零，都需要删除对应client的缓存以及/sn/bridgeclient



##### 3.4 ETCD访问

​	state-manager在初始化的时候会初始化MetaDataEtcdClient，但是没有初始化RouteDataEtcdClient，此次需求需要新增初始化RouteDataEtcdClient，这个地方需要修改state-manager的deployment.yaml，新增RouteEtcd的环境变量

​	后续bridgeManager直接通过Get方法获取client去访问和监听不同的持久化路径



##### 3.5 从持久化数据恢复缓存

​	bridgeManager初始化的时候，需要从ETCD中的持久化数据恢复缓存，所需要的数据，均可以从此次新增的持久化路径中获取，这样避免直接对/sn/workers和/sn/functions进行全量读取操作。

​	监听的时候，虽然会监听/sn/functions和/sn/workers，但是会对每一个事件进行判断，如果是/sn/functions，会通过函数元数据中的isBridgeFunction标识来确认是否执行后续动作；如果是/sn/workers，分两种情况：

（1）对于bridgefunc的监听，监听的是除了delete事件之外的其他的事件，会判断对应实例的stateId是否在缓存中，是的话才执行后续动作。

（2）对于client的监听，监听的只有delete事件，判断client对应的pod是否在缓存中，是的话才执行后续动作。



##### 3.6 插件构建

​	需要修改构建流程，在编译state-manager的时候编译出bridgeplugin.so，在构建state-manager镜像的时候，将该库放置在/home/sn路径下

​	state-manager在初始化的时候，bridgeHandler中会进行插件的加载，这样保证只有主State-Manager会进行插件的初始化



##### 3.7 client Crash的情况

​	监听到这一事件后，走release流程中 bridgeManager需要额外删除etcd和redis里面的stateId记录，因为这个场景不是从functiontask触发，不会terminate流程，需要保证把在引用计数归零的时候把bridgefunc缩容，因此需要删除etcd和redis里面的记录



### 4 遗留问题

#### 4.1 频繁冷启

​	当前用户侧发起release，由于bridgeManager是作为被动的调用方，当前接口返回给functiontask是个bool值，表明是否需要释放bridgefunc。而在用户侧，微服务场景下用的spring框架，servicebridge提供的JDBC API中，会在close中发起release，这就导致事务顺序执行场景下，即使两个事务之间间隔很短，也还是会触发bridgefunc的缩容，造成频繁的冷启动，极大影响性能。需要考虑延时缩容的机制，既不会导致functiontask发起的请求超时，又能保证在一定时间间隔内的两次事务能尽可能复用同一个bridgefunc

​	当前考虑有几种方案

（1）构建一个延时消费的队列，release请求返回的不能仅仅是一个bool，期望是发起release后，假设判断这个bridgefunc要被缩容，将它放到队列，此时如果并发有新的getBridgeFunc调用过来且正好分配到这个bridgefunc，那么引用计数就不再为0了，此时要将它移出队列，之后release接口结束前要开始消费这个队列，此时留在队首的就应该是确定要缩的bridgefunc，然后把这个bridgefunc返回回去

（2）servicebridge的SDK中，close内部不再调用release，这样好处是只有client函数退出，manager才会把对应bridgefunc的引用计数归零，在client存在的整个生命周期里，都能避免二次冷启，由于这个场景不是从functiontask触发的，等同于client crash的时候的处理

（3）servicebridge的SDK中，内部搞一个伪连接池，用户通过JDBC 的connection.close的时候不会真正触发release，而是在spring退出的时候才真正触发release调用，这个时候才会真正触发缩容



### 可靠性场景：

1、client函数实例、BridgeFunc函数实例异常退出

worker-manager通过监听k8s的实例信息可以获取到对应事件，并将相应路由etcd中的实例信息删除，BridgeManager插件通过监听路由ETCD，可以获取异常退出的事件并更新内部维护关系。

 

2、BridgeManager插件与ETCD连接中断、网络抖动、访问超时

state-manager初始化会初始化ETCD的client，已有相关的可靠性，包括感知client异常时，关闭client、关闭监听channel并重新建立client和监听channel

 

3、BridgeManager异常重启、State-Manager组件异常重启

- 由于State-Manager为主备部署，在现网环境为三实例，因此插件在初始化时需要保证只有主State-Manager会进行插件的初始化
- BridgeManager异常重启表明state-manager进程异常重启，此时会触发重新选主，只需保证State-Manager切主时能进行插件的初始化即可



4、内部client实例-stateid-BridgeFunc实例关系可靠性保障

- 由于当前相关数据为内存中持有，当重启时会导致数据丢失，需要将其持久化至ETCD中，并在插件初始化时从ETCD中恢复相关数据
- 恢复后需要重新从ETCD中查找client实例信息、BridgeFunc实例信息，以确保数据的准确性
- 数据的校验避免全量读取操作，避免对ETCD产生瞬时的过大压力



5、FunctionTask向BridgeManager插件申请stateKey异常

状态路由创建失败，主调函数会直接返回错误，不会产生异常流程

 

6、BridgeFunc函数实例扩容、特化失败

由于stateID与对应有状态实例绑定，一次扩容失败会导致当次主调函数请求异常，再次触发函数调用会重新请求扩容

 

7、client函数实例与BridgeFunc函数实例之间请求通信失败

当前有状态互调场景下，无异常重试机制，单次通信失败，client函数会接收到错误返回，需要内部进行相关逻辑处理

 

8、serviceBridge场景较强依赖于有状态函数、函数互调特性及能力，相关可靠性由已有功能保证，同时需要保证融合方案不对已有有状态相关功能产生影响，此可靠性由全量用例保证