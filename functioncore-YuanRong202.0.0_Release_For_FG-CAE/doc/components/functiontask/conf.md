# conf.json 配置项说明

基本格式参考[conf.json](../taskimpl/functioncore/conf.json)

下列配置项会被functiontask上层使用到：
1. level：functiontask上层日志等级
    1. 日志等级从低到高,日志数量从少到多依次为：Error, Warn, Info, Debug；
1. call_timeout：表示functiontask内部调用call或者callondemand接口的最长等待时间
    1. 有效范围`[5s, 100*24*3600s]`（该有效范围由sn-core约束）
1. max_wait_future_cnt：表示functiontask支持wait的最大snfture数量，默认为1024
1. resolve_deadlock：解决死锁
    1. 如果该选项为false，则不支持解决死锁
1. persistent_state：状态外置，影响可靠性
    1. 如果该选项为false，则不支持可靠性，不支持setTimeout，不支持查找和删除service下的所有状态
1. persistent_future：外置future
    1. 如果该选项为false，则不支持可靠性
1. future_ttl：内存中的future超时时间；
    1. 当future完成后，再经过future_ttl的时间，会将该future回收
    1. 默认为15分钟
1. external_future_ttl: 外置future超时时间；
    1. 默认为86400秒(1天)
    1. 当外置future完成后，在经过external_future_ttl的时间，会将该future回收

其余配置项functiontask上层不感知，由sn-core使用
1. state_server_addr表示环境中redis集群的访问地址
