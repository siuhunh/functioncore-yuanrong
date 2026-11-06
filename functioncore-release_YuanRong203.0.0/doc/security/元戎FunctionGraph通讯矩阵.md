[TOC]

# 通讯矩阵

|                 源设备                  |         源IP         |   源端口  |            目的设备             |  目的IP   |    目的端口    |  协议     |                          端口说明                           |  侦听端口是否可更改   | 认证方式  | 加密方式 |  所属平面    |  版本   | 特殊场景  |  备注   |
|:------------------------------------:| :------------------: | :------: |:---------------------------:| :------: |:----------:| :------: |:-------------------------------------------------------:| :----------------: |:-----:|:----:| :------: | :-------: | :-----: | :-----: |
|     FunctionGraph组件：rdispatcher      | rdispatcher组件对接VPC终端节点IP | 随机 |      FunctionAccessor       | 集群Node IP |   31220    | https |     FunctionAccessor服务端口，通过kube-proxy实现负载均衡，集群外部访问      | 是 | AK/SK | TLS  |数据面 | | | |
|                  NA                  |NA|NA|      FunctionAccessor       |服务Cluster IP|    8888    |https|     FunctionAccessor服务端口，通过kube-proxy实现负载均衡，集群内部访问      | 是 | AK/SK | TLS  |数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|      FunctionAccessor       |POD IP|    8888    |https|                FunctionAccessor服务端口，健康检查                | 是 |   无   | TLS  |数据面 | | | |
| FunctionGraph组件：functionflow-invoker | functionflow-invoker服务出口IP |随机|          workflow           |workflow所在节点Node IP|   32334    |https|                      workflow对外端口                       | 是| AK/SK | TLS  |数据面 | | | |
| FunctionGraph组件：functionflow-manager | functionflow-manager服务出口IP |随机|          workflow           |workflow所在节点Node IP|   32334    |https|                      workflow对外端口                       | 是| AK/SK | TLS  |数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|          workflow           |POD IP|    9992    |https|                          健康检查                           | 是 |   无   | TLS  |数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|       workflowmonitor       |POD IP|   28917    |https|                          健康检查                           | 是 |   无   | TLS  |数据面 | | | |
|                  NA                  |NA|NA|          workflow           |服务Cluster IP|    9992    |https|         workflow服务端口，通过kube-proxy实现负载均衡，集群内部访问          | 是 | AK/SK | TLS  |数据面 | | | |
|            Function Task             |POD IP|随机|  Function Instance Manager  |服务Cluster IP|   58866    |https| Function Instance Manager服务端口，通过kube-proxy实现负载均衡，集群内部访问 | 是 | AK/SK | TLS  |数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|  Function Instance Manager  |POD IP|   58867    |https|           Function Instance Manager服务端口，健康检查            | 是 |   无   | TLS  |数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|        State Manager        |POD IP|    8087    |https|                 State Manager服务端口，健康检查                  | 是 |   无   | TLS  |数据面 | | | |
|            Function Task             |POD IP|随机|        State Manager        |服务Cluster IP|    8087    |https| State Manager服务端口，通过kube-proxy实现负载均衡，集群内部访问 | 是 | AK/SK | TLS  |数据面 | | | |
|                Worker                |POD IP|随机|        cache manager        |服务Cluster IP|    9993    |https|       cache manager服务端口，通过kube-proxy实现负载均衡，集群内部访问       | 是 |   无   | TLS  |数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|        cache manager        |POD IP|    9993    |https|                 cache manager服务端口，健康检查                  | 是 |   无   | TLS  |数据面 | | | |
|           FunctionAccessor           |FunctionAccessor服务出口IP|随机|        Function Task        |Function Task所在节点Node IP|   22423    |https|                    Function Task对外端口                    | 是 | AK/SK | TLS  |数据面 | | | |
|            其他FunctionTask            |POD IP|随机|        FunctionTask         |FunctionTask所在节点Node IP|   32568    |tcp|                   Function Task节点间通信                    | 是 |   无   | TLS  | 数据面 | | | |
|            FunctionTask              |POD IP|随机|        worker         |FunctionTask所在节点Node IP|   32569    |tcp|                   init阶段支持worker与bus间通信                    | 是 |   无   | TLS  | 数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|        Function Task        |POD IP|    8888    |https|                          健康检查                           | 是 |   无   | TLS  |数据面 | | | |
|             FunctionTask             |PodIP|随机|           worker            |PodIP|   22668    |tcp|                   worker 与 busProxy建链                   | 是 | AK/SK | TLS  |数据面 | | | |
|              kubernetes              |服务Cluster IP|随机|           worker            |POD IP|    8081    |https|                          健康检查                           | 是 |   无   | TLS  |数据面 | | | |
|                Worker                |127.0.0.1|随机|       runtime-manager       |127.0.0.1|   31530    |tcp|                    runtime-manager端口                    | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31531    |tcp|                  nodejs6.10 runtime端口                   | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31532    |tcp|                  nodejs8.10 runtime端口                   | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31533    |tcp|                  nodejs10.16 runtime端口                  | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31534    |tcp|                  nodejs12.13 runtime端口                  | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31535    |tcp|                   python2.7 runtime端口                   | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31536    |tcp|                   python3.6 runtime端口                   | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31537    |tcp|                     java8 runtime端口                     | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|   31538    |http/tcp|                    custom runtime端口                     | 是 |   无   |  无   |数据面 | | | |
|                Worker                |127.0.0.1|随机|           runtime           |127.0.0.1|    8000    |http/tcp|                      web runtime端口                      | 是 |   无   |  无   |用户面 | | | |
|           FunctionAccessor           |FunctionAccessor服务出口IP|随机|            etcd             |etcd所在ecs节点IP|    2379/32379    |https|                        etcd对外端口                         | 是 | 账号密码  | TLS  |数据面 | | | |
|            Function Task             |Function Task服务出口IP|随机|            etcd             |etcd所在ecs节点IP|    2379/32379    |https|                        etcd对外端口                         | 是 | 账号密码  | TLS  |数据面 | | | |
|            Cache Manager             |Cache Manager服务出口IP|随机|            etcd             |etcd所在ecs节点IP|    2379/32379    |https|                        etcd对外端口                         | 是 | 账号密码  | TLS  |数据面 | | | |
|      Function Instance Manager       |Function Instance Manager服务出口IP|随机|            etcd             |etcd所在ecs节点IP|    2379/32379    |https|                        etcd对外端口                         | 是 | 账号密码  | TLS  |数据面 | | | |
|            State Manager             |State Manager服务出口IP|随机|            etcd             |etcd所在ecs节点IP|    2379/32379    |https|                        etcd对外端口                         | 是 | 账号密码  | TLS  |数据面 | | | |
|      Function Instance Manager       |Function Instance Manager服务出口IP|随机|       华为云DCS服务（redis）       |华为云DCS实例IP|    6379    |tcp|                        redis对外端口                        | 是 | 账号密码  |  无   |数据面 | | | |
|             State Manager            |State Manager服务出口IP|随机|       华为云DCS服务（redis）       |华为云DCS实例IP|    6379    |tcp|                        redis对外端口                        | 是 | 账号密码  |  无   |数据面 | | | |
|               workflow               |workflow服务出口IP|随机|            etcd             |etcd所在ecs节点IP| 2379/32379 |https|                          访问端口                           | 是 | 账号密码  | TLS  |数据面 | | | |
|               workflow               |workflow服务出口IP|随机|        华为云rds(mysql)        |华为云rds实例IP|    3306    |http/tcp|                          访问端口                           | 是 | 账号密码  |  无   |数据面 | | | |
|               workflow               |workflow服务出口IP|随机| FunctionGraph组件：rdispatcher | rdispatcher组件对接的vpc终端节点IP |   28902    |https|                     rdispatcher对外端口                     | 是 | AK/SK | TLS  |数据面 | | | |
| FunctionAccessor |FunctionAccessor服务出口IP|随机| faas-scheduler | POD IP | 8889 |https| faas-scheduler对外端口 | 是 | AK/SK | TLS | 数据面 | | | |
| FunctionAccessor |FunctionAccessor服务出口IP|随机| stream-handler | POD IP | 28939 |https| stream-handler对外通信端口 | 是 | token | TLS | 数据面 | | | |
| FunctionAccessor |FunctionAccessor服务出口IP|随机| stream-handler | POD IP | 28917 |https| stream-handler健康检查端口 | 是 | token | TLS | 数据面 | | | |
| Worker |Worker服务出口IP|随机| stream-handler | POD IP | 28939 |https| stream-handler对外通信端口 | 是 | token | TLS | 数据面 | | | |
| kubernetes |服务Cluster IP|随机| faas-scheduler | POD IP | 9994 |https| faas-scheduler健康检查端口 | 是 | 无 | TLS | 数据面 | | | |
| kubelet | worker所在节点IP |随机| worker | POD IP | 8082 |http| worker preStop状态检查端口 | 是 | 无 | 无 | 数据面 | | | kubelet版本低于1.26时不支持HTTPS |