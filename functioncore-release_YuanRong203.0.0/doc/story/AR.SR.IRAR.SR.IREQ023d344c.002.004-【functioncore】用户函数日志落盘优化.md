[TOC]

# 【functioncore】用户函数日志落盘优化

## 1. 需求场景分析

### 1.1 特性需求来源与价值概述

当前用户日志未能实时打印，影响用户调试和体验。

### 1.2特性场景分析

当前用户日志未能实时打印，缓存到2K 大小才会落盘打印，不能做到实时的打印用户日志，影响用户长时间执行函数中日志打印使用。

### 2.1总体方案

#### 当前日志打印流程：

各种runtime的用户日志打印接口：

```
func (r *LogRecorder) Write(funcLog *FunctionLog) {
	r.logsMux.Lock()
	r.logs.Write(funcLog)
	r.logsMux.Unlock()
}
```

打印日志逻辑如下：

```
func (r *FixSizeRecorder) Write(funcLog *FunctionLog) {
	if r.logs == nil {
		r.logs = list.New()
	}

	if r.size > r.limitSize {
		for {
			elem := r.logs.Front()
			if elem == nil {
				break
			}
			funcLog, ok := elem.Value.(*FunctionLog)
			if !ok {
				break
			}

			size := r.guessSize(funcLog)
			if r.size-size < r.limitSize {
				break
			}

			r.size -= size
			r.dropped(funcLog, false)
			r.logs.Remove(elem)
		}
	}

	size := r.guessSize(funcLog)
	if size > r.limitSize { // special case: the funcLog is too large
		r.Range(func(fl *FunctionLog) {
			r.dropped(fl, false)
		})
		r.logs = nil
		r.size = 0
		r.dropped(funcLog, true)
	} else {
		r.logs.PushBack(funcLog)
		r.size += size
	}
}
```

大于limitSize才会调用dropped才会将日志落盘。

dropped函数中主要调用当前异步日志框架，进行日志落盘。

当调用结束时，会对当前缓存中的日志进行全部落盘。

#### 当前优化方案：

由于之前日志有做过一个需求要求load和init阶段的日志traceID要和invoke阶段的traceID一致，根据这个需求采用以下两种分支：

<1> 在load和init阶段采用之前缓存的日志落盘。

<2> invoke阶段，修改原来的缓存的落盘方式，直接调用异步落盘的logger写日志接口。500ms或0.5M大小落盘。



### 2.3 约束

#### 2.3.1上下游依赖需求

#### 2.3.2其他约束

## 3. 可信软件

### 3.1安全性（Security） 

不涉及

*【系统对恶意威胁的防护能力。可能的威胁包括但不限于未授权访问、使用、泄露、破坏、修改、毁灭；防护目标是保持信息的机密性、完整性和可用性】* 

### 3.2韧性（Resilience）

不涉及

*【系统受攻击时保持在有定义的运行状态（包括降级）、遭遇攻击时快速恢复核心系统、受攻击后在业务需要的时间窗内恢复到有效运行态】* 

### 3.3隐私性（Privacy） 

*【系统具备能力能使客户（含组织和个人）能够就收集、使用、保有、披露和处置个人信息履行其权利和义务】*
不涉及隐私

### 3.4可靠性（Reliability）

 可靠性相关与之前计费流程一样，所以没有涉及新增，与之前的连接functiontask失败、连接redis失败等可靠性场景一致，原有可靠性场景已经覆盖，未新增上报的逻辑。

*【在给定的条件、给定的时间区间能无失效地执行要求的能力】*

### 3.5可用性（Availability）

不涉及

*【系统可操作和可访问的程度，或能实现其指定系统功能的概率】*

### 3.6安全（Safety）

不涉及

*【系统失效导致的危害不存在不可接受的风险，不会伤害自然人生命或危及自然人健康，不管是直接还是通过损害环境或财产间接造成的* 】

### 3.7可维护性

不涉及

*【系统的可修复(恢复)性和可改进性的难易程度】*

### 3.8可测试性

*【代码本地LLT易测试、针对外部接口测试、测试用例具有稳定性】*

### 3.9性能

不涉及

*【评估需求本身的性能影响、对其他流程的性能影响】*

### 3.10资源

不涉及

*【评估需求对资源的占用，例如go协程，文件句柄、内存、cpu】*

### 3.11资料

不涉及

*【客户可感知的需求，需要输出相应资料】*

### 3.12升级兼容性

不涉及

*【是否兼容之前的版本、是否支持单组件独立升级】*

## 4. 用例

| **序号** | **用例名**         | 用例类型 | **测试步骤**                                                 | **预期结果**     |
| -------- | ------------------ | -------- | ------------------------------------------------------------ | ---------------- |
| 1        | 用户日志实时打印   | ST       | 配置一个长时间执行的函数，函数中实时打印，看日志是否可以实时输出 | 用户日志实时输出 |
| 2        | 当前日志的用例覆盖 |          |                                                              |                  |



