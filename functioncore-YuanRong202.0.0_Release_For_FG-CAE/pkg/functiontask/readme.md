# FunctionTask

## 代码目录结构

- docs  文档
- task  独立于core-sdk、sn-cli和test，实现task的相关功能和接口
    - pkg  对外接口
        - adaptor      编程模型的接口对接至通用的消息处理函数
        - api          编程模型的接口在function task的功能实现
        - common       公共变量，结构体定义
        - config       task的配置
        - efs          external future store，实现task可靠性需要将future放到外置存储
        - future       future内存中的实现，提供New，GetUntil，Get，Peek，Set，Clear等功能
        - futurestore  内存中的future store，维护future的集合
        - log          日志
        - stateclient  读写状态的抽象接口
        - statequeue   核心模块，实现了状态队列
        - taskcontext  请求进入task，会为该请求构建一个taskcontext，用于记录该请求的上下文，提供一些该请求的方法
        - util         工具函数
- taskimpl  task具体实现，依赖task目录
    - functioncore  基于functioncore的实现
        - cmd           main函数
            - functiontask    task的main函数，初始化task
        - internal      非对外接口
            - coreadaptor      适配core-sdk的消息处理函数
            - coreconfig       适配core-sdk的配置
            - corecontext      适配core-sdk的请求上下文
            - coreinvoker      适配core-sdk的执行函数接口
            - corestateclient  适配core-sdk的状态读写接口
            - router           http路由，task性能分析对外接口
            - stateload        状态负载上报
    - localtask         有状态函数的本地运行功能
            - adaptor          与sn-cli交互的接口
            - localadaptor     适配sn-cli的消息处理函数
            - localcontext     适配sn-cli的请求上下文
            - localinvoker     适配sn-cli的执行函数接口
            - localstateclient 适配sn-cli的状态读写接口
- test  通过mock进行的复杂测试，如api功能，可靠性等
    - api           接口的功能测试
    - reliability   接口的可靠性测试
    - testcontext   用于测试的请求上下文

## 本地编码环境配置

```sh
export GO111MODULE=on
export GOPROXY="http://mirrors.tools.huawei.com/goproxy/,https://cmc.centralrepo.rnd.huawei.com/go/"
export NO_PROXY=${NO_PROXY},*.huawei.com
export GONOSUMDB=*
```

[插件安装](../../doc/functioncore/functiontask/tools.md)

提交代码前需要做这些检查
1. 保存文件时触发gofmt, goimports, golint, golangci-lint
2. StaticCheck, GoMetrics
3. 运行单元测试，检查代码覆盖率是否超过70%

## 编译二进制

```sh
cd taskimpl/functioncore
make
```

## 编译镜像

```
cd $haresvs/build/functiontask
sh build.sh -i
```

## 日志等级依据

- debug: 调试时有用的信息，不会在生产环境中显示
- info: 生产环境需要显示的正常信息，重要模块的日志输出，以粗粒度级别突出显示关键信息。如无新增模块，则info等级的日志应该基本保持不变。
    - adaptor
        - 消息到来
        - 消息处理完成
        - 发送call
        - 收到call的结果
    - state queue
        - 创建请求
        - 调度请求
        - 暂停请求（get，wait）
        - 完成请求
    - future store
        - 创建future
        - 完成future
        - 删除future
- warn: 告警，需要有人确认问题，但是不紧急，明天再干也没事；或者是用户请求有问题，非系统错误
    - 用户请求参数错误
    - 用户函数出错
    - 调用底层接口失败，但是问题不大，如保存状态第一次失败，可以重试
- error: 系统错误，但是系统仍然能够正常运行，需要根据日志`立即`定位问题，日志需要尽可能的详细
    - 系统定义的序列化和反序列化失败
    - 调用底层接口失败，并且无规避手段，如保存状态重试3次还是失败
