---
layout: global
displayTitle: Quick Start
title: Quick Start
description: Wormhole Quick Start page
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

   **新建database，对于Kafka系统，database对应Kafka中的Topic，配置Topic名称即可**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-database-create.png" alt="" width="600"/>

   **新建namespace，选中Kafka Topic后，填写table名字，相当于为当前Topic下的数据分类，可自定义名称，table名字后面须配置数据的唯一标识字段**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-namespace-create.png" alt="" width="600"/>

**3. Kafka 集群中创建 source topic，并生成测试数据。若 Source Namespace 为 `kafka.test.source.ums_extension.*.*.*`，则生成Kafka消息时应设置消息的key为 `data_increment_data.kafka.test.source.ums_extension.*.*.*`。生成测试数据命令如下：**

   ```
cd /usr/local/kafka/bin
./kafka-console-producer.sh --broker-list localhost:9092 --topic source --property "parse.key=true" --property "key.separator=@@@"
data_increment_data.kafka.edp.source.ums_extension.*.*.*@@@{"id": 1, "name": "test", "phone":"18074546423", "city": "Beijing", "time": "2017-12-22 10:00:00"}
   ```

**4. Admin 配置 Source Namespace Schema**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-schema.png" alt="" width="600"/>

   **注意：若ums_ts_字段设置为long类型，对应数值应该为时间对应的秒数或毫秒数。**
**5. Admin 创建 Sink Namespace**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-sink-instance-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-sink-database-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-sink-namespace-create.png" alt="" width="600"/>   

**6. Admin 创建 Lookup Namespace**

   **Lookup Instance/Database 配置图省略**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-lookup-ns.png" alt="" width="600"/>

**7. Admin 创建 UDF**

    UDF为可选项，可以省略该步骤，JAR包需要自己编译生成，具体方式参考[User Guide](https://edp963.github.io/wormhole/user-guide.html) 章节

   **创建Spark UDF需要将jar包放置到配置文件中HDFS中的udfjars目录下，即spark.wormhole.hdfs.root.path/udfjars**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-udf-spark.png" alt="" width="600"/>

   **Flink支持UDF和UDAF操作，创建Flink UDF/UDAF需要将相关jar包放置到Flink安装目录中的lib下**

​    **注：Wormhole Flink UDF支持普通的java程序，而不需要按照Flink官方文档的格式实现UDF。UDF/UDAF使用例程请参考 [User Guide](https://edp963.github.io/wormhole/user-guide.html) 章节**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-udf-flink.png" alt="" width="600"/>

**8. Admin 创建 Project 并授权 Namespaces 和 Users**

   **将 Source Namespace， Sink Namespace， Lookup Namespace 和 demo User 授权给 Project**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-project.png" alt="" width="600"/>

## User 用户

**Wormhole支持Spark和Flink两种流式处理引擎，下面分别介绍Spark和Flink Stream/Flow的使用流程。**

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

   **启动 Flow， 将 Source Namespace 对应 Topic 信息，Flow 配置信息发送给 Stream**

  <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start.png" alt="" width="600"/>

**3. 提前创建Lookup Table，Sink Table**

**注：sink table 中应有`id,name,cardBank,age,city`字段**

**4. User 启动 Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-start.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-running.png" alt="" width="600"/>

**Stream切换到running状态后，若出现数据写不进去，Flow状态为failed等问题，请在 Yarn Application页面上查看 Stream  Driver/Executor日志**

**5. Flow漂移**

**支持Flow从一个Stream到另一个Stream的漂移。点击相应Flow中漂移按钮后，填写目标Stream即可。注：（1）只有spark default flow可以迁移，其他Flow不能迁移；（2）只能迁移至与原Flow对应Stream消费同一Kafka集群的Stream，即对应kafka instance url相同**

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-draft.png" alt="" width="600"/>

**具体Flow漂移规则介绍请参考[User Guide](https://edp963.github.io/wormhole/user-guide.html) 章节**

**启动过程中有问题可先参考[FAQ](https://edp963.github.io/wormhole/faq.html)章节排查~~**

### Flink Stream/Flow 使用流程

**1. User 登录系统后创建 Stream**

​   **新建 Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-stream-flink.png" alt="" width="600"/>

   **配置 Stream 资源**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-configs-flink.png" alt="" width="600"/>

**2. User 启动 Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-stream-running-flink.png" alt="" width="600"/>

**3. User 创建并启动 Flow**

   **flow的创建分三个步骤，分别为Pipeline、Transformation、Confirmation。这三个步骤分别对应的是配置flow的pipeline,配置flow的transformation逻辑，以及最终确认配置，并将配置好的flow提交到wormhole中。下面，对这三个步骤中涉及到的信息进行简要说明。**

 **（1）Pipeline配置：需要对Stream、Source、Sink三部分进行配置**

   **选择 Flink Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-stream-flink.png" alt="" width="600"/>

   **选择数据源**

   **Flink Stream/Flow支持消费UMS类型和用户自定义类型（UMS_extension）数据**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-source-flink.png" alt="" width="600"/>

   **如上图，Source Namespace为`kakfa.test.flinksource.source.*.*.*`，发送测试数据命令如下：**

   ```
./kafka-console-producer.sh --broker-list localhost:9092 --topic flinksource --property "parse.key=true" --property "key.separator=###" 
data_increment_data.kafka.test.flinksource.source.*.*.*###{"protocol":{"type":"data_increment_data"}，"schema":{"namespace":"kafka.test.flinksource.source.*.*.*"，"fields":[{"name":"ums_id_"，"type":"long"，"nullable":false}，{"name":"ums_ts_"，"type":"datetime"，"nullable":false}，{"name":"ums_op_"，"type":"string"，"nullable":false}，{"name":"key"，"type":"int"，"nullable":false}，{"name":"value1"，"type":"string"，"nullable":true}，{"name":"value2"，"type":"long"，"nullable":false}]}，"payload":[{"tuple":["1"，"2016-04-11 12:23:34.345123"，"i"，"10"，"aa1"，"10"]}，{"tuple":["2"，"2016-04-11 15:23:34.345123"，"u"，"10"，"aa2"，"11"]}，{"tuple":["3"，"2016-04-11 16:23:34.345123"，"d"，"10"，"aa3"，"12"]}]}
data_increment_data.kafka.test.flinksource.source.*.*.*###{"protocol":{"type":"data_increment_data"}，"schema":{"namespace":"kafka.test.flinksource.source.*.*.*"，"fields":[{"name":"ums_id_"，"type":"long"，"nullable":false}，{"name":"ums_ts_"，"type":"datetime"，"nullable":false}，{"name":"ums_op_"，"type":"string"，"nullable":false}，{"name":"key"，"type":"int"，"nullable":false}，{"name":"value1"，"type":"string"，"nullable":true}，{"name":"value2"，"type":"long"，"nullable":false}]}，"payload":[{"tuple":["1"，"2016-04-11 12:23:34.345123"，"i"，"11"，"aa4"，"13"]}，{"tuple":["2"，"2016-04-11 15:23:34.345123"，"u"，"11"，"aa5"，"14"]}，{"tuple":["3"，"2016-04-11 16:23:34.345123"，"d"，"11"，"aa6"，"15"]}]}
data_increment_data.kafka.test.flinksource.source.*.*.*###{"protocol":{"type":"data_increment_data"}，"schema":{"namespace":"kafka.test.flinksource.source.*.*.*"，"fields":[{"name":"ums_id_"，"type":"long"，"nullable":false}，{"name":"ums_ts_"，"type":"datetime"，"nullable":false}，{"name":"ums_op_"，"type":"string"，"nullable":false}，{"name":"key"，"type":"int"，"nullable":false}，{"name":"value1"，"type":"string"，"nullable":true}，{"name":"value2"，"type":"long"，"nullable":false}]}，"payload":[{"tuple":["1"，"2016-04-11 12:23:34.345123"，"i"，"12"，"aa7"，"13"]}，{"tuple":["2"，"2016-04-11 15:23:34.345123"，"u"，"12"，"aa8"，"15"]}，{"tuple":["3"，"2016-04-11 16:23:34.345123"，"d"，"12"，"aa9"，"16"]}]}
   ```

   **选择目标端**

   **Flink Stream/Flow支持异构sink**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sink-flink.png" alt="" width="600"/>

  **（2）Transformation 配置**

​	**配置完flow的pipeline之后，点击浮框下方的”下一步”按钮，即可进入Transformation的设置。这是flow配置的关键步骤，需要在这一步中明确数据的转换方式。通过点击Transformation标签旁的”点击修改”按钮来进行Transformation设置。**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-flink-transform.png" alt="" width="600"/>

​   **CEP，复杂事件处理**

​	**Transformation选择CEP，这里填入的CEP基本信息为 Windowtime:30（S）； Strategy:NO_SKIP  ；keyBy:key；Output:Detail**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-cep-flink.png" alt="" width="600"/>

 	  **CEP中pattern设置**

  	 **填写完CEP的基本信息后，通过点击”添加Pattern”按钮，可以添加多条Pattern，每条Pattern的具体内容都需要在第二层弹出框中进行设置。在这里，填入的内容如下**

  <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-cep-pattern-begin-flink.png" alt="" width="600"/>

​	 **在填写完Pattern之后，配置具体效果如图所示**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-cep-result-flink.png" alt="" width="600"/>

 	**Flink CEP 配置结果**

 	**点击”保存”按钮后，弹框关闭，之前的所有编辑都被保存成一条CEP Transformation记录，在原来的Transformation处显示出来。支持Processing time和Event time两种模式。（说明：由于wormhole支持一个flow内有多个CEP及Lookup SQL、Flink SQL的混合编排，因此，这里将显示一张表，记录了所有配置完毕的Transformation记录）**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-transform-result-flink.png" alt="" width="600"/>

**Flink中通过中Transformation Config设置“exception_process_method”字段可选择对流处理中异常信息的处理方式。现在能捕获读取kafka后数据预处理、lookup操作、写sink操作时的异常。处理方式有unhandle、interrupt、feedback三种。**

**注意：当在配置文件中设置checkpoint为true，则异常处理不能设置为interrupt，否则flow会一直重启。**

 **（3）提交flow**

**配置完flow的Transformation后，通过点击浮框下方的”下一步”按钮，来进入到Confirmation步骤，对flow进行提交前的最后确认。当确认所有填写内容都没有问题后，就可以点击浮框下方的”提交”按钮来提交刚刚配置好的这个flow。否则，可以通过点击浮框下方的”上一步”按钮来返回之前的步骤，对错误的配置信息进行修改。**

**启动 Flow， 配置Topic Offset信息**

   **Stream处于running状态时，才可以启动Flow**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start-conf-flink.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start-flink.png" alt="" width="600"/>


**demo过程中有问题可先参考[FAQ](https://edp963.github.io/wormhole/faq.html)章节排查~~**
