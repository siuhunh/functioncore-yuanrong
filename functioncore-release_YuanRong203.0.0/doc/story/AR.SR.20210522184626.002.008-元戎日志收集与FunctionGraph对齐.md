# AR.SR.20210522184626.002.008-元戎日志收集与FunctionGraph对齐

## 1. Story 整体设计
### 1.1 功能描述
- **背景** <br/>
  当前输出的日志文件目录的格式没有与FunctionGraph对齐，导致日志采集工具采集不到日志文件，导致维测定位效率低
- **方案** <br/>
  使用K8s的HostPath功能挂载主机目标目录到容器日志目录，使日志文件输出到与FunctionGraph对齐的指定目录

### 1.2 场景分析
  日志根据挂载的目录不同，分为worker业务日志，runtime-manager日志，runtime业务日志，用户函数日志，组件日志。对此分场景挂载到宿主机

### 1.3 方案详细设计
- **挂载方式** <br/>
  使用K8s的hostPath方式，将宿主机的指定目录挂载卷到容器中。则会将日志同时输出到挂载的宿主机目录中<br/>

- **目录权限修改方式** <br/>
  hostPath会导致kubelet将挂载的目录属主属组修改为root:root，会导致组件没有权限向日志目录写入日志文件；在基础镜像中引入sudo命令并在dockerfile中增加init脚本的sudo权限，在entrypoint中执行init脚本修改目录属主属组

- **宿主机大目录指定** <br/>
  与FunctionGraph对齐为/var/paas/sys/log/cff/default，按照要求指定日志输出目录格式<br/>

- **worker业务日志（包括实例相关的.info文件）** <br/>
  将宿主机大目录/servicelogs/{podname}挂载到worker容器的/home/sn/log/service目录<br/>

- **runtime-manager日志** <br/>
  将宿主机大目录/servicelogs/{podname}挂载到组件容器的/home/sn/log目录。<br/>
  
- **用户函数日志** <br/>
  将宿主机大目录/processrouters/stdlogs/{podname}挂载到worker容器的/home/sn/log/user目录<br/>

- **runtime业务日志** <br/>
  将宿主机大目录/servicelogs/{podname}挂载到到runtime-manager容器的/home/snuser/log目录<br/>

- **组件日志** <br/>
  包括functionAccessor、functionInstanceManager、functiontask、cachemanager、workflow。将宿主机大目录/componentlogs/{podname}挂载到组件容器的/home/sn/log目录<br/>
  
- **采集到的的日志** <br/>
  采集到之后，将从界面LTS服务中看到日志。日志文件滚动压缩由cffagent负责。
  
目录汇总：

| 日志类型  | 所在组件 | 容器内目录 | 宿主机目录  |
| ------------ | ------------ | ------------ | ------------ |
| 组件日志 | functionAccessor、functionInstanceManager、functiontask、cachemanager、workflow | /home/sn/log | /var/paas/sys/log/cff/default/componentlogs/{podname} |
| 服务日志 | worker | /home/sn/log/service | /var/paas/sys/log/cff/default/servicelogs/{podname} |
| 服务日志 | runtime-manager | /home/sn/log | /var/paas/sys/log/cff/default/servicelogs/{podname} |
| 服务日志 | runtime-manager | /home/snuser/log | /var/paas/sys/log/cff/default/servicelogs/{podname}/runtime |
| 用户日志 | worker | /home/sn/log/user | /var/paas/sys/log/cff/default/processrouters/stdlogs/{podname} |

### 1.4 上下游依赖需求
  与管理面日志采集联调，目标能够采集到并读到正确格式的日志。对接人潘炜程

## 2. 可信软件
### 2.1 安全性（Security）
*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】*  
  日志文件全部600权限，不影响安全性

### 2.2 韧性（Resilience）
*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】*  
不涉及
### 2.3 隐私性（Privacy）
*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*  
不涉及
### 2.4 可靠性（Reliability）
*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*  
不涉及
### 2.5 可用性（Availability）
*【处于按要求执行状态的能力】*  
不涉及
### 2.6 安全（Safety）
*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的】*  
不涉及
## 3. HLT 用例
| 序号 | 用例名 | 测试步骤 | 预期结果 |
| ------------ | ------------ | ------------ | ------------ |
| 1 |  正确采集日志 | 调用函数，查看采集出的所有组件和用户函数的日志 | 采集并读到所有组件和用户函数的正确格式日志 |
