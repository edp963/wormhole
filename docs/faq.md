---
layout: global
displayTitle: FAQ
title: FAQ
description: Wormhole Concept page
---

* This will become a table of contents (this text will be scraped).
{:toc}
### Stream 

#### Stream启动时提示resource is not enough

Stream启动时提示resource is not enough，说明project分配的资源不足，需要admin用户登陆后，增大project分配的memory和cpu资源

#### Spark Stream 一直处于 starting 状态

1. 在页面上查看stream日志，根据日志上的错误信息排查问题，一般是spark配置、目录权限、用户权限等问题。如果是权限问题，请按照部署文档说明执行deploy.sh脚本或根据提示手动修复。

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/faq-stream-log.png" alt="" width="600"/>

2. 检查application.conf中配置的spark路径是否正确，检查Wormhole服务启动用户是否有权限访问该目录。

3. 查看Wormhole服务后台日志application.log中是否有启动失败提示。

4. 按照上面顺排查问题后，手动执行SQL将Wormhole服务数据库中该stream状态设置为failed。

   ```
   mysql client

   update stream set status = "failed" where id = 1;  // 1换成对应stream id
   ```

5. 重启wormhole服务，重启stream。

6. 如果yarn上该stream为running状态，wormhole页面显示仍为starting状态，可检查yarn集群的时钟和wormhole部署机器的时钟是否同步，如果时钟不同步，状态更新可能会存在问题。

**若以上步骤仍不能解决问题，请及时反馈~~**

**注意: Flow suspending状态代表挂起状态，标识Flow信息已注册到Stream中，Stream目前处于非running状态。Stream状态正常后Flow状态会自动切换到running或failed状态。具体请查看Stream/Flow部分文档。**


#### CDH版Spark消费Kafka报错

1. 错误信息

  ```
  Exception in thread "streaming-start" java.lang.NoSuchMethodError: org.apache.kafka.clients.consumer.KafkaConsumer.subscribe(Ljava/util/Collection;)V
	at org.apache.spark.streaming.kafka010.Subscribe.onStart(ConsumerStrategy.scala:85)
	at org.apache.spark.streaming.kafka010.WormholeDirectKafkaInputDStream.consumer(WormholeDirectKafkaInputDStream.scala:55)
	at org.apache.spark.streaming.kafka010.DirectKafkaInputDStream.start(DirectKafkaInputDStream.scala:242)
  ```

  解决办法：

  - CDH版本Spark默认消费Kafka版本是0.9，需要修改SPARK_KAFKA_VERSION值为相应Kafka大版本，如0.10
  - CDH管理页面部署方式，通过页面修改SPARK_KAFKA_VERSION参数
  
    <img src="https://github.com/edp963/wormhole/raw/master/docs/img/faq-cdh-spark-config.png" alt="" width="600"/>

  - 自己手动安装方式，须在Wormhole所在机器的环境变量中添加`export SPARK_KAFKA_VERSION=0.10`



#### Flink Stream 一直处于 starting 状态

1. 在页面上查看stream日志，根据日志上的错误信息排查问题，一般是目录权限、用户权限等问题。如果是权限问题，请按照部署文档说明执行deploy.sh脚本或根据提示手动修复。

2. 如果没有stream日志，一般是配置有问题。查看启动Wormhole服务对应Console日志，目前已知错误与解决方案如下。

   ```
   Exception in thread "main" java.lang.NoClassDefFoundError: com/sun/jersey/core/util/FeaturesAndProperties
   ```
   解决办法: 

   HDP版本Hadoop/Yarn集群解决方案，添加环境变量

   ```
   export HADOOP_CLASSPATH=`hadoop classpath`
   ```
   
3. 检查application.conf中配置的flink路径是否正确，检查Wormhole服务启动用户是否有权限访问该目录。

4. 查看Wormhole服务后台日志application.log中是否有启动失败提示。

5. 按照上面顺排查问题后，手动执行SQL将Wormhole服务数据库中该stream状态设置为failed。

   ```
   mysql client

   update stream set status = "failed" where id = 1;  // 1换成对应stream id
   ```

6. 重启wormhole服务，重启stream。

**若以上步骤仍不能解决问题，请及时反馈~~**


### Flow

#### Spark Flow running状态，sink端接收不到数据

1. 检查Yarn上对应Stream的driver/executor日志，看有没有错误信息。

2. 若没有错误，从日志上查看Stream消费的offset范围，可能期间没有新数据，可生成新的测试数据或者使用Stream 生效按钮调整offset。

3. 检查测试数据消息key是否与flow sourcenamespace匹配。若flow sourcenamespace为`kafka.kafka01.source.user.*.*.*`，则生成kafka消息时，key应设置为`data_increment_data.kafka.kafka01.source.user.*.*.*`。

4. flow transformation配置后可选择在日志上sample show几条数据，查看是否因为逻辑问题导致。

   备注：sink的数据库需要创建ums_id_ （long型），ums_active_ （int型）和ums_ts_（datetime型）三个系统字段，用来做幂等用。 

#### Flink Flow一直处于starting状态

1. 检查Yarn上对应Stream的job日志，看有没有错误信息，检查是否有task生成，当没有有效数据的时候，不会形成task，flow的状态会一直为starting
2. 检查是否有有效数据，查看flow消费的offset范围，检查offset范围内的测试数据消息key是否与flow sourcenamespace匹配。若flow sourcenamespace为`kafka.kafka01.source.user.*.*.*`，则生成kafka消息时，key应设置为`data_increment_data.kafka.kafka01.source.user.*.*.*`。key和namespace匹配时数据才为有效数据

### Job

#### Job创建时，version选择失败

1. 检查wormhole application.log中是否有异常
2. 检查wormhole部署的机器是否配置了HADOOP_HOME环境变量
3. 检查hdfs上是否有该namespace对应的数据
4. 检查hdfs上该namespace对应的数据，第五位是否为数字