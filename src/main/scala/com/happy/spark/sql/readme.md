**Shark** HQL-->RDD操作，然后ive元数据获取获取数据库里的表信息<-->hdfs数据和文件
优点：因为转化为了rdd操作，快  不足：更多是对hive的改造，替换了hive的物理引擎，继承了大量hive代码，优化和维护问题。
随着性能优化和先进分析整合的进一步发展，其中的mr部分成为整个项目的瓶颈。

**Hive** HQL 翻译成Mapper-Reducer-Mapper的代码，产生MapReduce的Job；MR代码以及相关资源打成Jar包，自动发布到Hadoop集群中运行。
Hive架构：
> * Driver   sql -- driver -- compiler(解释为mr任务)， compiler将sql转化为plan（仅包含元数据操作（只包含ddl语句）和hdfs操作（只包含LOAD语句）组成）  解析-语义分析-逻辑计划-优化-执行
> * 3种服务模式
> * MetaStore
> * Hadoop

Spark SQL CLI:以本地方式运行在Hive的元数据服务上

LogicalPlan:
QueryPlan: 