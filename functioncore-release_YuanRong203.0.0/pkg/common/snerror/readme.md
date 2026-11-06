#SN错误码与约束

-	错误描述信息
1.	英文,带标点符号的完整的句子，都小写,建议统一使用华为翻译，不要散装英语
2.	做到一码一个错误描述，错误提示的有效参数信息可以拼接
3.	用户行为的错误描述信息不可包含组件信息
4.	错误信息避免打印IP等系统敏感信息
5.	异常描述不要携带组件名称等特殊含义的字段
6.	公共类型的错误，需要先在公共类库中查找，避免单个引用
7.	错误码做到语言内唯一，不要反复定义
8.	不需要对外暴露的异常，统一用一个错误，但是日志中需要记录清楚错误原因。
9.  组件内的错误码后四位，建议按照约束处理
    如  120500  组成是  12（repo）+0(不重试，1表示重试)+500（后端服务不可预知的错误）
        121404  组成是  12（repo）+1(重试，0表示重试)+404（资源不存在类错误）

- commonError

    公共的异常33开头，6位数字
    
- UserError用户错误

  新增的用户错误，都需要在这里维护
  
    _* 表示 描述信息动态生成,参考出现场景及解决办法_
    
    | 错误号 | 错误提示 | 错误场景 | 解决方法 |
    | --- | --- | --- | --- |
    |管理态错误：
    |4101|function name is already exist |函数已存在|rename your function.|
    |4102|function alias is already exist |函数别名已存在|rename your function alias.|
    |4103|total routing weight is not 100 |权重之和需要是100|check the routing weight. the sum of the values is 100.|
    |4104| * |输入参数有误|check input parameters.|
    |4105| this version is occupied by an alias |版本已存在别名|remove the mapping first.|
    |4106| too many layer versions |层版本太多|delete or resize.|
    |4107| tenant cannot create layer |租户没有创建权限||
    |4108| too big layer max version |层版本号太大||
    |4109| the number of triggers exceeds the upper limit. |触发器数量太多|delete or resize limit.|
    |4111| the number of existing function alias is greater than the set value. |函数别名太多|delete or resize limit.|
    |4112| layer is used |层已经被使用|cancel the use before performing the operation.|
    |函数调用错误：
    | 4001   | function entry cannot be found     | 找不到函数入口 | 检查函数入口配置   |
    | 4002   | function invocation exception      | 函数调用异常   | 检查用户函数       |
    | 4003   | state content is too large         | 状态信息太大   | 检查状态信息       |
    | 4004   | function return value is too large | 函数返回值太大 | 检查函数代码返回值 |
    | 4005   | state is undefined                 | 状态未定义     | 检查状态信息       |
    | 4006   | function name has no service ID    | 函数不是有状态函数| 检查函数是否配置了server ID|
    | 4007   | function result is invalid  | 函数调用结果非法| 根据错误提示检查函数代码|
