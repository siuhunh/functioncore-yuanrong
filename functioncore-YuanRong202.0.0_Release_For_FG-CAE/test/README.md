- ### 使用说明

```shell
# 挂载本地源码和本地go test相关测试工具到容器目录
docker run -ti -v /home/YuanRong:/home -v /home/disk/gopath:/home/disk/gopath docker-hub.tools.huawei.com/sn_staticcheck/coverage:v2.3 "/bin/sh"

# 进入容器测试目录
docker exec -ti ${containerID} "/bin/sh"
cd /home/functioncore/test/

# 准备测试环境
source /etc/profile
sed -i "s/10.162.202.103/100.107.90.218/g" functiontask/test/reliability/redis.go

# 运行测试脚本
bash -x test.sh all   # 运行所有模块
bash -x test.sh ${MODULE_NAME}  # 运行单个模块(function-repository、http-access等，具体可参见test.sh脚本里MODULE_LIST数组定义)

# 查看运行结果，输出结果在对应${MODULE_NAME}的output目录下，当前使用语句覆盖模式，支持html查看

```

- ### 测试目录树

测试目录树如下所示，后续要新加测试模块请参照执行

```shell
test.sh  
├── common
│   ├── output
│   └── test.sh
├── frontend-invoke
│   ├── output
│   └── test.sh
├── cache-manager
│   ├── output
│   └── test.sh
├── functionbus
│   ├── output
│   └── test.sh
├── functioncore
│   ├── log.json
│   ├── package.json
├── functionstate
│   ├── output
│   └── test.sh
├── functiontask
│   ├── output
│   ├── readme.md
│   ├── test
│   └── test.sh
├── state-manager
│   ├── output
│   └── test.sh
├── test.sh
├── worker
│   ├── output
│   └── test.sh
└── worker-manager
    ├── output
    └── test.sh
```

- ### 优化

1. ##### **当前LLT模块之间有依赖，后续可优化无依赖之后，可并行测试所有模块，减少运行时间**

2. ##### **尽量起容器来测试**



