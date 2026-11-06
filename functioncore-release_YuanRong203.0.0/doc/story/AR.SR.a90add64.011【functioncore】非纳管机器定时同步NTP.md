[TOC]

# 【functioncore】非纳管机器定时同步NTP

关联AR:
+ AR.SR.a90add64.011【functioncore】非纳管机器定时同步NTP


## 1. Story 整体设计

### 1.1 功能描述

#### 1.1.1 背景

- 源于BUG单：元数据etcd 节点状态，ntp 偏移量大于150s，无告警产生，请定位排查，同时请排查现网，workflow etcd存在同样问题。https://clouddevops.huawei.com/#/bug/BUG2022033102367

#### 1.1.2 场景分析

- 数据面资源账号无法选择元数据etcd节点，无法搜到，导致ECS无法配置ntp告警。

#### 1.1.3 解决方案
- 采用规避方案，在非纳管节点上增加守护巡查进程，定时判断时钟偏移大于一定阈值。上报告警，同时自动同步时钟。


### 1.2 详细设计

#### 1.2.1 新增文件
- **巡查进程：ntp_check.go，生成可执行二进制巡查程序**，部署于非纳管机器上定期巡查ntp时差
- **配置文件：ntp_check.cfg**，由巡查进程读取，做到关键数据可配置
  - omsvc的addres由部署人员填写，用于上报告警url的组成

  - ntp_server可配置1~10个ntp服务器地址

  - 巡查间隔interval(单位s)，最大允许时差max_offset秒，巡查失败连续alarm_cnt次后上报告警

  - 告警内容可由alarm.content配置

  - ssl_path目录下需包含trust.cer、server.cer、server_key.pem、cert_pwd内容，用于告警签名

  - cipher_path目录下包含common_shared.key、root.key，用于告警签名过程

  - 配置正确的Akey、Skey内容

    ```shell
    [omsvc]
    addres = 100.85.218.231:28915
    
    [ntp_server]
    # support addres0 addres1 ... addres10
    addres0 = ntp01-in.huawei.com:123
    addres1 = ntp02-in.huawei.com:123
    addres2 = ntp03-in.huawei.com:123
    
    [check]
    interval   = 5
    alarm_cnt  = 1
    max_offset = 150
    
    [alarm]
    content = "The NTP offset is greater than 150 seconds."
    
    [tls]
    tls_protocol = TLSv1.2
    tls_ciphers  = TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384
    AKey        = 0000000100000001810D3C1E268671F67ADD3323A1E56DA0AE45C7FBA2D36B3982E0D2EFA05EF061B03F7082661C3100D430497D6D634524
    SKey        = 00000001000000010C7AF3378BC502DCEBED0DC7F0FF997CA1BA35937310174AC54B02B49D7438BBA9FB33BF56CD29F93DC8FDAF1EE4A0B67709C1A2BE720F27D20B48BB4D119E1C
    ssl_path    = /opt/CFF/etc/ssl
    cipher_path = /opt/CFF/etc/cipher
    ```
#### 1.2.2 交互过程
1. 运行进程```nohup ./ntp_check &``` 
2. 按interval时间的周期性与ntp服务器交互获取时间
3. 按需发送告警、消警消息至omsvc节点
    ```go
    // 伪代码逻辑示意
    cnt        = 0     // 错误次数
    alarm_flag = false // 告警发送标志
    
    每隔interval s巡查一次:
      if ntp偏移量 > max_offset:
          cnt++
          纠正时间
          if cnt >= alarm_cnt && alarm_flag == false: 
              发送告警成功，alarm_flag = true
      else:
          cnt = 0
          if alarm_flag == true:
              发送消警成功，alarm_flag = false
    ```

[运行模型-顺序图-Open Model](https://cloudmodeling.tools.huawei.com/projectspace/detail/f29373794c59405eaeecb87f3c553847?projectDiagramUUID=7e871aeabc0f4262a97a4a3af2176607&versionUUID=)![运行模型-顺序图](https://cloudmodelingapi.tools.huawei.com/cloudmodelingdrawiosvr/d/2DEY?t=1657181791980)


#### 1.2.3 告警、消警消息 构造及发送
- 告警消息构造参考pkg\common\alarm\alarm.go，仿照该模块调用者构造告警、消警消息内容并发送至OMSVC


#### 1.2.4 部署过程
- 二进制可执行程序ntp_check、配置文件ntp_check.cfg放置在/home/etcd/etcd-v3.4.10-linux-amd64/Ntp_check/下
  ```shell
  Ntp_check/
  ├── ntp_check
  └── ntp_check.cfg
  ```

- 在etcd元数据机器的bin/start.sh脚本中添加./ntp_check的启动
  ```shell
  ...
  # start ntp_check
  if [ "$(pgrep -f "./ntp_check" |wc -l)" -gt 0 ]; then
      echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Ntp_check has already started"
  else
      cd "$CURR_DIR"/../Ntp_check && nohup ./ntp_check &
      cd -
  fi
  
  echo "[INFO][$(date +%b\ %d\ %H:%M:%S)]:Starting etcd server: $LOCAL_IP"
  ...
  ```

- 守护进程脚本monitor.sh在/etc/crontab由linux系统定时任务定期调用，在monitor.sh的start()中添加对于./ntp_check的监控，若进程死亡则重新使用start.sh拉起

  ```shell
  function start()
  {
      ...
      ntp_check_pid=$(ps -wwef | grep ntp_check | grep -v grep | awk '{print $2}')
      if [[  ! -z "${pids}" && ! -z "${ntp_check_pid}" ]]; then
          echo "The etcd&ntp_check process exists"
          exit 1
      fi
      
      sh ${curpath}/start.sh
      ...
  }
  ```
#### 1.2.4 日志系统
- 日志系统使用functioncore的pkg\common\logger模块
- ./ntp_check运行后，在/home/sn/log/ntpCheck-run.log生成日志文件

### 1.3 上下游依赖需求

【描述本次设计模块和其他模块的交互关系，以及同其它模块的依赖关系】
不涉及

## 可信软件

### 2.1 安全性（Security）

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  
不涉及

### 2.2 韧性（Resilience）

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*  
不涉及

### 2.3 隐私性（Privacy）

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*  
不涉及

### 2.4 可靠性（Reliability）

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*  
1. ntp服务器支持配置10个，在单个ntp服务器失效时，从0~9号依次访问其他服务器
2. 支持告警消息发送失败后的重试
3. 支持守护进程对特性进程的定期巡查，若死亡可及时重新拉起

### 2.5 可用性（Availability）

*【处于按要求执行状态的能力】*  
1. 对配置项进行合法性校验，以防出现alarm_cnt < 0类似的非法配置输入

### 2.6 安全（Safety）

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及

## 3. HLT 用例

| 序号  | 用例名   | 测试步骤          | 预期结果                                                            |
|-----|-------|---------------|-----------------------------------------------------------------|
| 1   | ntp巡查 | 配置文件正确，正确执行进程 | 进程能够定期向ntp server请求比对时间，偏移量大于max_offset时能够上报告警至omsvc，时间正常后能正确消警 |
