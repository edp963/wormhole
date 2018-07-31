---
layout: global
displayTitle: Quick Start
title: Quick Start
description: Wormhole WH_VERSION_SHORT Quick Start page
---

{:toc}

**本章节以一个流式项目的实施示例介绍 Wormhole 页面的使用流程。实施过程中可参考 Tutorial 相关章节。**

**业务需求：实时处理 Kafka 中数据，处理过程中关联 Mysql 数据库某表，然后转换过滤数据，写入 Mysql 系统中。**

## Admin 用户

**1. Admin 用户登录系统后创建普通用户**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-createUser.png" alt="" width="600"/>

**2. Admin 创建 Source Namespace**

​    **新建instance**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-instance-create.png" alt="" width="600"/>

   **新建database, 对于Kafka系统，database对应Kafka中的Topic**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-database-create.png" alt="" width="600"/>

   **新建namespace, 选中Kafka Topic后, 填写table名字, 相当于为当前Topic下的数据分类, table名字后面配       置数据的唯一标识字段**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-namespace-create.png" alt="" width="600"/>

**3. Kafka 集群中创建 source topic，并生成测试数据。若 Source Namespace 为 `kafka.test.source.ums_extension.*.*.*`，则 生成Kafka 消息时应设置消息的key为 `data_increment_data.kafka.test.source.ums_extension.*.*.*`。生成测试数据命令如下：**

   ```
cd /usr/local/kafka/bin
./kafka-console-producer.sh --broker-list hdp1:6667 --topic source --property "parse.key=true" --property "key.separator=@@@"
data_increment_data.kafka.edp.source.test.*.*.*@@@{"id": 1,"name": "test","phone":"18074546423","address": "Beijing","time": "2017-12-22 10:00:00"}
   ```

**4. Admin 配置 Source Namespace Schema**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick_start-source-schema.png" alt="" width="600"/>

**5. Admin 创建 Sink Namespace**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-sink-instance-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-sink-database-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-sink-namespace-create.png" alt="" width="600"/>   

**6. Admin 创建 Lookup Namespace**

   **Lookup Instance/Database 配置图省略**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-lookup-ns.png" alt="" width="600"/>

**7. Admin 创建 Project 并授权 Namespaces 和 Users**

   **将 Source Namespace, Sink Namespace, Lookup Namespace 和 demo User 授权给 Project**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-project.png" alt="" width="600"/>

## User 用户

​    **Wormhole支持Spark和Flink，下面分别对Spark项目和Flink项目建立流程进行介绍。**

### Spark Stream/Flow 使用流程

**1. User 登录系统后创建 Stream**

​    **新建 Stream**           

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-stream.png" alt="" width="600"/>

   **配置 Stream 资源**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-configs.png" alt="" width="600"/>

**2. User 创建并启动 Flow**

   **选择 Spark Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-stream.png" alt="" width="600"/>

   **选择数据源**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-source.png" alt="" width="600"/>

   **选择目标端**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sink.png" alt="" width="600"/>

   **配置 Sink 类型，Source Namespace 中数据只配置了 "ums_ts_" 系统字段，"mutation_type" 只能设置为 "i"，即 "insert only"。具体介绍请参考 [Concept](https://edp963.github.io/wormhole/concept.html) 和 [User Guide](https://edp963.github.io/wormhole/user-guide.html) 章节**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sinkConfig.png" alt="" width="600"/>

   **配置数据转换逻辑，即 Transformation 配置**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-transform.png" alt="" width="600"/>

   **配置Lookup SQL，流上 Source Namespace 关联 MySQL userCard 表**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-lookupSql.png" alt="" width="600"/>

   **Lookup SQL 配置结果**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-transform-result.png" alt="" width="600"/>

   **Spark SQL，过滤部分字段**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sparkSql.png" alt="" width="600"/>

   **Spark SQL 配置结果**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sparkSql-result.png" alt="" width="600"/>

   **启动 Flow, 将 Source Namespace 对应 Topic 信息，Flow 配置信息发送给 Stream**

  <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start.png" alt="" width="600"/>

**3. 提前创建Lookup Table，Sink Table**

**注：sink table 中应有`id, name, cardBank, age, city, ums_ts_`字段**

**4. User 启动 Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-start.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-running.png" alt="" width="600"/>

**Stream切换到running状态后，若出现数据写不进去，Flow状态为Failed等问题，请在 Yarn Application页面上查看 Stream  Driver/Executor日志**

**具体步骤及配置说明请参考其他章节~~**

### Flink Stream/Flow 使用流程

**1. User 登录系统后创建 Stream**

​   **新建 Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-stream-flink.png" alt="" width="600"/>

   **配置 Stream 资源**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-configs-flink.png" alt="" width="600"/>

**2. User 启动 Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-running-flink.png" alt="" width="600"/>

**3. User 创建并启动 Flow**

   **选择 Flink Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-stream-flink.png" alt="" width="600"/>

   **选择数据源**
   
   目前Flink Stream/Flow只支持消费UMS类型数据，创建新Source Namespace并授权访问权限，无须配置Source Schema，向Kafka中发送UMS类型测试数据。
   
   如图，Source Namespace为`kakfa.test.flinksource.source.*.*.*`，发送测试数据命令如下：
   ```
   ./kafka-console-producer.sh --broker-list localhost:9092 --topic flinksource --property "parse.key=true" --property "key.separator=###" 
    data_increment_data.kafka.test.flinksource.source.*.*.*###{"protocol":{"type":"data_increment_data"},"schema":{"namespace":"kafka.test.flinksource.source.*.*.*","fields":[{"name":"ums_id_","type":"long","nullable":false},{"name":"ums_ts_","type":"datetime","nullable":false},{"name":"ums_op_","type":"string","nullable":false},{"name":"key","type":"int","nullable":false},{"name":"value1","type":"string","nullable":true},{"name":"value2","type":"long","nullable":false}]},"payload":[{"tuple":["1","2016-04-11 12:23:34.345123","i","10","aa1","10"]},{"tuple":["2","2016-04-11 15:23:34.345123","u","10","aa2","11"]},{"tuple":["3","2016-04-11 16:23:34.345123","d","10","aa3","12"]}]}
    data_increment_data.kafka.test.flinksource.source.*.*.*###{"protocol":{"type":"data_increment_data"},"schema":{"namespace":"kafka.test.flinksource.source.*.*.*","fields":[{"name":"ums_id_","type":"long","nullable":false},{"name":"ums_ts_","type":"datetime","nullable":false},{"name":"ums_op_","type":"string","nullable":false},{"name":"key","type":"int","nullable":false},{"name":"value1","type":"string","nullable":true},{"name":"value2","type":"long","nullable":false}]},"payload":[{"tuple":["1","2016-04-11 12:23:34.345123","i","11","aa4","13"]},{"tuple":["2","2016-04-11 15:23:34.345123","u","11","aa5","14"]},{"tuple":["3","2016-04-11 16:23:34.345123","d","11","aa6","15"]}]}
    data_increment_data.kafka.test.flinksource.source.*.*.*###{"protocol":{"type":"data_increment_data"},"schema":{"namespace":"kafka.test.flinksource.source.*.*.*","fields":[{"name":"ums_id_","type":"long","nullable":false},{"name":"ums_ts_","type":"datetime","nullable":false},{"name":"ums_op_","type":"string","nullable":false},{"name":"key","type":"int","nullable":false},{"name":"value1","type":"string","nullable":true},{"name":"value2","type":"long","nullable":false}]},"payload":[{"tuple":["1","2016-04-11 12:23:34.345123","i","12","aa7","13"]},{"tuple":["2","2016-04-11 15:23:34.345123","u","12","aa8","15"]},{"tuple":["3","2016-04-11 16:23:34.345123","d","12","aa9","16"]}]}
   ```

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-source-flink.png" alt="" width="600"/>

   **选择目标端**
   
   目前Flink Flow目标数据源只支持Kafka

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sink-flink.png" alt="" width="600"/>

   **配置 Sink 类型**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sinkConfig.png" alt="" width="600"/>

   **配置数据转换逻辑，即 Transformation 配置**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-transform-flink.png" alt="" width="600"/>

​   **CEP，复杂事件处理**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-cep-flink.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-cep-pattern-begin-flink.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-cep-pattern-next-flink.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-cep-result-flink.png" alt="" width="600"/>

   **Flink CEP 配置结果**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-transform-result-flink.png" alt="" width="600"/>

   **启动 Flow, 配置Topic Offset信息**
   
   Stream处于running状态时，才可以启动Flow

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start-conf-flink.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start-flink.png" alt="" width="600"/>


**demo过程中有问题可先参考[FAQ](https://edp963.github.io/wormhole/faq.html)章节排查~~**