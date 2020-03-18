---
layout: global
displayTitle: Deployment
title: Deployment
description: Wormhole Deployment page
---

{:toc}

## 前期准备

#### 环境准备

- JDK1.8
- Hadoop-client（HDFS，YARN）（支持版本 2.6+）
- Spark-client （支持版本 2.2.0，2.2.1）(若使用 Spark Streaming 引擎，须部署 Spark-client)
- Flink-client （wormhole 0.6.1 之前版本支持 flink 1.5.1 版本，wormhole 0.6.1 及之后版本支持 flink 1.7.2 版本）(若使用 Flink 引擎，须部署 Flink-client)

#### 依赖服务

- Hadoop 集群（HDFS，YARN）（支持版本 2.6+）
- Zookeeper
- Kafka （支持版本 0.10.2.2）
- Elasticsearch（支持版本 5.x）（非必须，若无可通过在配置文件中配置 mysql 查看 wormhole 处理数据的吞吐和延时）
- MySQL

#### Jar 包准备

mysql-connector-java-{your-db-version}.jar

## 部署配置

#### 下载安装包

**下载 wormhole-0.6.3.tar.gz 包 (链接:https://pan.baidu.com/s/1nDzcHYKIVCT6atq9wAuK9Q  密码:5cqn)，或者自编译 mvn clean install -Pwormhole**

```
下载wormhole-0.6.3.tar.gz安装包
tar -xvf wormhole-0.6.3.tar.gz
或者自编译，生成的tar包在 wormhole/target
git clone -b 0.6 https://github.com/edp963/wormhole.git
cd wormhole
mvn install package -Pwormhole
```

**注: [若前端需要自编译，并且 node_modules 依赖包下载失败，可参考链接中issue](https://github.com/edp963/wormhole/issues/1008#issuecomment-491172069)**

#### 配置环境变量

**配置 SPARK_HOME/HADOOP_HOME 环境变量**

#### spark-client 下配置文件示例

```
spark-env.conf

export JAVA_HOME=/usr/local/jdk
export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_HOME=/usr/hdp/current/hadoop-client
```

#### flink-client 下配置文件示例

```
flink-conf.yaml

env.java.home: /usr/local/jdk
env.java.opts: -XX:+UseConcMarkSweepGC -XX:+PrintGCDetails -XX:-UseGCOverheadLimit -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/tmp/wormhole/gc
yarn.application-attempts: 2
```

#### rider-server 配置文件

**修改 application.conf 配置文件**

```
conf/application.conf 配置项介绍



akka.http.server.request-timeout = 120s

wormholeServer {
  cluster.id = "" #optional global uuid
  host = "localhost"
  port = 8989
  ui.default.language = "Chinese"
  token.timeout = 1
  token.secret.key = "iytr174395lclkb?lgj~8u;[=L:ljg"
  admin.username = "admin"    #default admin user name
  admin.password = "admin"    #default admin user password
  refreshInterval = "5"  #refresh yarn to update stream status interval(second)
}

mysql = {
  driver = "slick.driver.MySQLDriver$"
  db = {
    driver = "com.mysql.jdbc.Driver"
    user = "root"
    password = "root"
    url = "jdbc:mysql://localhost:3306/wormhole?useUnicode=true&characterEncoding=UTF-8&useSSL=false"
    numThreads = 4
    minConnections = 4
    maxConnections = 10
    connectionTimeout = 3000
  }
}

ldap = {
  enabled = false
  user = ""
  pwd = ""
  url = ""
  dc = ""
  read.timeout = 3000
  read.timeout = 5000
  connect = {
    timeout = 5000
    pool = true
  }
}

spark = {
  spark.home = "/usr/local/spark"
  yarn.queue.name = "default"        #WormholeServer submit spark streaming/job queue
  wormhole.hdfs.root.path = "hdfs://nn1/wormhole"   #WormholeServer hdfslog data default hdfs root path
  yarn.rm1.http.url = "localhost:8088"    #Yarn ActiveResourceManager address
  yarn.rm2.http.url = "localhost:8088"   #Yarn StandbyResourceManager address
  #yarn.web-proxy.port = 8888    #Yarn web proxy port, just set if yarn service set yarn.web-proxy.address config
}

flink = {
  home = "/usr/local/flink"
  yarn.queue.name = "default"
  checkpoint.enable = false
  checkpoint.interval = 60000
  stateBackend = "hdfs://nn1/flink-checkpoints"
  feedback = {
    enabled = false
    state.count = 100
    interval = 30
  }
}

zookeeper = {
  connection.url = "localhost:2181"  #WormholeServer stream and flow interaction channel
  wormhole.root.path = "/wormhole"   #zookeeper
}

kafka = {
  brokers.url = "localhost:6667"         #WormholeServer feedback data store
  zookeeper.url = "localhost:2181"
  topic.refactor = 3
  using.cluster.suffix = false #if true, _${cluster.id} will be concatenated to consumer.feedback.topic
  consumer = {
    feedback.topic = "wormhole_feedback"
    poll-interval = 1m
    poll-timeout = 1m
    stop-timeout = 30s
    close-timeout = 20s
    commit-timeout = 70s
    wakeup-timeout = 1h
    max-wakeups = 10000
    session.timeout.ms = 120000
    heartbeat.interval.ms = 50000
    max.poll.records = 1000
    request.timeout.ms = 130000
    max.partition.fetch.bytes = 4194304
  }
}

#kerberos = {
#  kafka.enabled = false
#  keytab = ""
#  rider.java.security.auth.login.config = ""
#  spark.java.security.auth.login.config = ""
#  java.security.krb5.conf = ""
#}

# choose monitor method among ES、MYSQL
monitor = {
  database.type = "ES"
}

#Wormhole feedback data store, if doesn't want to config, you will not see wormhole processing delay and throughput
#if not set, please comment it
#elasticSearch.http = {
#  url = "http://localhost:9200"
#  user = ""
#  password = ""
#}


#delete feedback history data on time
maintenance = {
  mysql.feedback.remain.maxDays = 7
  elasticSearch.feedback.remain.maxDays = 7
}


#Dbus integration, support serveral DBus services, if not set, please comment it
#dbus = {
#  api = [
#    {
#      login = {
#        url = "http://localhost:8080/keeper/login"
#        email = ""
#        password = ""
#      }
#      synchronization.namespace.url = "http://localhost:8080/keeper/tables/riderSearch"
#    }
#  ]
#}

```

#### Spark版本配置

wormhole 0.6.2及之后版本增加spark不容版本兼容，已适配spark2.2/2.3/2.4，默认支持的是spark2.2，其他版本需要用户修改代码中的spark版本号进行适配。

##### 适配spark2.3

（1）将parent pom中的properties：

`<spark.extension.version>2.3</spark.extension.version>`

（2）修改启动stream时调用的sparkx jar包名称

修改RiderConfig.scala中wormholeJarPath 和wormholeKafka08JarPath 变量

```
lazy val wormholeJarPath = getStringConfig("spark.wormhole.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-ums_1.3-sparkx_2.3-0.6.1-jar-with-dependencies.jar")
lazy val wormholeKafka08JarPath = getStringConfig("spark.wormhole.kafka08.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-ums_1.3-sparkx_2.3-0.6.1-jar-with-dependencies-kafka08.jar")
```

##### 适配spark2.4

（1）将parent pom中的properties：

```
<spark.extension.version>2.4</spark.extension.version>
<json4s.version>3.5.3</json4s.version>
```

（2）修改启动stream时调用的sparkx jar包名称

修改RiderConfig.scala中wormholeJarPath 和wormholeKafka08JarPath 变量

```
lazy val wormholeJarPath = getStringConfig("spark.wormhole.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-ums_1.3-sparkx_2.4-0.6.1-jar-with-dependencies.jar")
lazy val wormholeKafka08JarPath = getStringConfig("spark.wormhole.kafka08.jar.path", s"${RiderConfig.riderRootPath}/app/wormhole-ums_1.3-sparkx_2.4-0.6.1-jar-with-dependencies-kafka08.jar")
```

#### Flink 高可用配置

wormhole 0.6.1 及之后版本支持 flink 高可用配置。

（1）flink checkpoint 配置

如果 flink.checkpoint.enable=false 则不使用 checkpoint，默认为不适用。

如果使用 checkpoint 则需要配置 flink.checkpoint.enable=true，另外还可以设置 checkpoint 的间隔时间和存储系统。通过 flink.checkpoint.interval 可设置 checkpoint 的间隔时间，默认为 60000ms。通过 flink.stateBackend 可设置 checkpoint 的存储位置。

（2）flink 配置

配置 flink 高可用，需要配置 flink-conf.yaml 文件

```
high-availability: zookeeper

high-availability.storageDir: hdfs:///flink/ha/

high-availability.zookeeper.quorum: ip:port

high-availability.zookeeper.path.root: /flink
```

#### Feedback State 存储位置配置

wormhole 在 0.6 版本之前的 feedback state 默认存储在 ES 中，在 0.6 版本之后，将支持用户根据需求在 ES 与 MySQL 中间选择合适的存储库进行数据存储。如果需要将存储位置由 ES 迁往 MySQL，可以参照下面的步骤进行配置。通过配置 monitor.database.type 选择存储位置

`monitor.database.type = "MYSQL" #存储到mysql中`

`monitor.database.type = "ES" #存储到ES中`

当选择存储到 mysql 时，需要在 wormhole/rider/conf/wormhole.sql 新建 feedback_flow_stats 表，并在 wormhole 配置的数据库中执行该文件，从而在数据库中建立 feedback_flow_stats 表

#### Wormhole 集群部署

**部署说明**

wormhole 0.5.5-beta 及之后版本支持多套 wormhole 隔离部署

若只部署一套 Wormhole 可跳过此步骤

为支持同一 hadoop 集群环境中部署多套 Wormhole，在配置文件 conf/application.conf 中增加了 wormholeServer.cluster.id 参数（要求唯一）。单套 Wormhole 部署不设置 wormholeServer.cluster.id 或者 wormholeServer.cluster.id=""。为兼容之前版本，可不设置该变量。**注意：之前版本不要随意增加该参数，否则无法读取对应的 zookeeper 和 hdfs 信息，无法正常运行已配置的 stream 和 flow，即之前版本可以保持不变，新部署的 Wormhole 增加该参数即可。**

##### 单套 Wormhole 部署

- 单套 Wormhole 部署只需将 wormholeServer.cluster.id 设置为空或者不进行设置即可

  **说明**

- Kafka feedback topic：为 kafka.consumer.feedback.topic
- ES feedback index：为 elasticSearch.wormhole.feedback.index
- HDFS 路径为：spark.wormhole.hdfs.root.path
- Zookeeper 路径为：zookeeper.wormhole.root.path/cluster.id

##### 多套 Wormhole 隔离部署

- wormholeServer.cluster.id（必须配置）：每套 Wormhole 唯一的 uuid，不可重复
- kafka.using.cluster.suffix（选择设置）：该变量标记是否将 wormholeServer.cluster.id 作用于 kafka.consumer.feedback.topic。如果 kafka.using.cluster.suffix=false，则 feedback topic 为 kafka.consumer.feedback.topic；如果 kafka.using.cluster.suffix=true，则 feedback topic 为 kafka.consumer.feedback.topic + "\_" + cluster.id
- elasticSearch.wormhole.using.cluster.suffix（选择设置）：该变量标记是否将 wormholeServer.cluster.id 作用于 elasticSearch.wormhole.feedback.index。如果 elasticSearch.wormhole.using.cluster.suffix=false，则 feedback index 为 elasticSearch.wormhole.feedback.index ；如果 elasticSearch.wormhole.using.cluster.suffix=true，则 feedback index 为 elasticSearch.wormhole.feedback.index + "\_" + cluster.id

  **说明**

- MySQL：与 cluster.id 是否存在无关，所以部署多集群时，只要用不同的库的 url 即可
- Kafka feedback topic：参考上文 kafka.using.cluster.suffix 的设置
- ES feedback index：参考上文 elasticSearch.wormhole.using.cluster.suffix 的设置
- HDFS：spark.wormhole.hdfs.root.path 为一级根目录名，HDFS 路径为 spark.wormhole.hdfs.root.path/cluster.id
- Zookeeper：zookeeper.wormhole.root.path 为一级根目录名，Zookeeper 路径为 zookeeper.wormhole.root.path/cluster.id

#### Wormhole 接入 Kerberos认证的kafka 支持

wormhole 0.6.2 及之后版本支持接入 kerberos 支持。若无需接入 KerBeros 支持，可跳过此步骤

##### Flink 中 kerberos 认证

与 spark 不同，flink 在配置文件中实现对 kerberos 认证支持，仅需修改 flink/conf/flink-conf.yaml 文件，即可开启 flink 应用与 kerberos 集群的对接。flink-conf.yaml 具体配置为：

```
security.kerberos.login.keytab: keytab_path
security.kerberos.login.principal: principal_path
security.kerberos.login.contexts: client,kafkaClient
```

其中，security.kerberos.login.keytab 与 security.kerberos.login.principal 分别对应的是 kdc 服务器生成的 keytab 和 principal 文件的路径。security.kerberos.login.contexts 对应的是用户要对接的开启了 kerberos 认证的 kafka 集群与 zookeeper 集群

##### Wormhole 中 kerberos 认证

目前版本的 wormhole 支持接入 Kerberos认证的kafka集群

启用 kerberos 认证，需要在配置文件 application.conf 中对下列参数进行设置。参数及设置说明如下：

```
kerberos = {
  kafka.enabled = false                        #enable wormhole connect to Kerberized cluster
  keytab = ""                                  #the keyTab will be used on yarn
  rider.java.security.auth.login.config = ""   #the path of jaas config file which should be used by start.sh
  spark.java.security.auth.login.config = ""   #the path of jaas config file which will be uploaded to yarn
  java.security.krb5.conf = ""                 #the path of krb5.conf
}
```

##### 注意事项

在 kerberosized cluster 集群模式下，所有 kafka topic 都被严格控制创建、访问、写入权限，因此，一旦开启 kerberos 认证，wormhole 将不再支持在 kafka 集群中没有 wormhole_feedback 与 wormhole_heartbeat 这两个 topic 的情况下，自动创建这两个 topic 的操作。所以，需要用户联系 kafka 集群的管理人员，由他创建这两个 topic。

#### 授权远程访问

设置 wormhole server mysql 数据库编码为 uft8，并授权可远程访问

上传 mysql-connector-java-{version}.jar 至 \$WORMHOLE_HOME/lib 目录

须使用 application.conf 中 spark.wormholeServer.user 项对应的 Linux 用户启动服务，且须配置该 Linux 用户可通过 ssh 远程免密登录到自己

若配置 Grafana，Grafana 须配置可使用 viewer 类型用户匿名登陆，并生成 admin 类型的 token，配置在 \$WORMHOLE_HOME/conf/application.conf 中 grafana.admin.token 项中

切换到 root 用户，为 WormholeServer 启动用户授权读写 HDFS 目录，若失败，请根据提示手动授权

```
#将 hadoop 改为 Hadoop 集群对应的 super-usergroup
./deploy.sh --hdfs-super-usergroup=hadoop
```

## 启动停止

#### 启动

```
./start.sh

启动时会自动创建 table，kafka topic，elasticsearch index，grafana datasource，创建 kafka topic时，有时会因环境原因失败，须手动创建

topic name: wormhole_feedback partitions: 4
topic name：wormhole_heartbeat partitions: 1

# 创建或修改 topic 命令
./kafka-topics.sh --zookeeper localhost:2181 --create --topic wormhole_feedback --replication-factor 3 --partitions 4
./kafka-topics.sh --zookeeper localhost:2181 --create --topic wormhole_heartbeat --replication-factor 3 --partitions 1

./kafka-topics.sh --zookeeper localhost:2181 --alter --topic wormhole_feedback  --partitions 4
./kafka-topics.sh --zookeeper localhost:2181 --alter --topic wormhole_heartbeat  --partitions 1
```

#### 停止

```
./stop.sh
```

#### 重启

```
./restart.sh
```

**访问 http://ip:port 即可试用 Wormhole，可使用 admin 类型用户登录，默认用户名，密码见 application.conf 中配置**

## 升级

#### 0.6.1-0.6.2版本升级到0.6.3版本

（1）如果有使用custom class，需要按照新接口修改使用的custom class

（2）数据库操作

```
#instance表更新：
alter table `instance` add column `conn_config` VARCHAR(1000) NULL after `conn_url`;

#stream表更新：
alter table `stream` add column `special_config` VARCHAR(1000) NULL after `launch_config`;

#flow_history表更新：
alter table `flow_history` add column `config` VARCHAR(1000) NULL after `sink_ns`;

#flow表更新：
alter table `flow` add column `config` VARCHAR(1000) NULL after `sink_ns`;
下面的语句需要根据application.conf中自己的系统进行配置
update flow set config = CONCAT('{"parallelism":', parallelism,',"checkpoint":{"enable":false,"checkpoint_interval_ms":300000,"stateBackend":"hdfs://flink-checkpoint"}}') where parallelism is not null and stream_id in (select id from stream where stream_type='flink');
enable：代表是否启用checkpoint（true/false）
checkpoint_interval_ms：代表checkpoint的间隔时间
stateBackend：代表checkpoint保存的位置
```

修改完后，重启wormhole服务即可

#### 0.6.0 版本升级到 0.6.1 版本

（1）升级至 0.6.1 版本需要将 flink 升级为 1.7.2 版本

（2）删除数据库中 feedback_flow_stats 表。该表此次升级中结构改动较大，需要删除重建；该表记录 flow 的监控信息，删除后不影响现有业务运行，并且会在 wormhole 启动时重建

（3）数据库中 feedback_stream_offset、feedback_stream_error、feedback_flow_error 表已弃用，可自行删除

（4）重启 wormhole 服务后需要执行以下数据库操作

```
update flow set priority_id=0;
```

#### 0.5.3-0.5.5 版本升级到 0.6.0 版本

（1）数据库操作

```
#job表更新
alter table `job` add column `table_keys` VARCHAR(1000) NULL;
alter table `job` add column `desc` VARCHAR(1000) NULL;
update job,namespace set job.table_keys=namespace.keys where job.sink_ns like concat(namespace.ns_sys,".",namespace.ns_instance,".",namespace.ns_database,".",namespace.ns_table,'%');

#flow表更新
alter table `flow` add column `flow_name` VARCHAR(200) NOT NULL;
alter table `flow` add column `table_keys` VARCHAR(1000) NULL;
alter table `flow` add column `desc` VARCHAR(1000) NULL;
update flow,namespace set flow.table_keys=namespace.keys where flow.sink_ns like concat(namespace.ns_sys,".",namespace.ns_instance,".",namespace.ns_database,".",namespace.ns_table,'%');
update flow set flow_name=id;

#udf表更新
alter table `udf` add `map_or_agg` VARCHAR(100) NOT NULL;
update udf set map_or_agg='udf';
```

（2）停止所有 flow

在 0.6.0 版本启动之前，需停止以前版本所有 sparkx 的 flow（包括 starting、running、suspending、updating 状态的 flow）, 并记录当前 stream 消费到的 topic offset，重启 stream 时，手动设定从之前记录的 offset 消费

#### 0.5.0-0.5.2 版本升级到 0.6.0 版本

（1）数据库操作

```
#stream表更新
ALTER TABLE stream ADD COLUMN jvm_driver_config VARCHAR(1000) NULL;
ALTER TABLE stream ADD COLUMN jvm_executor_config VARCHAR(1000) NULL;
ALTER TABLE stream ADD COLUMN others_config VARCHAR(1000) NULL;
UPDATE stream SET jvm_driver_config=substring_index(stream_config,",",1);
UPDATE stream SET jvm_executor_config=substring_index(substring_index(stream_config,",",2),",",-1);
UPDATE stream SET others_config=substring(substring_index(stream_config,substring_index(stream_config,",",2),-1),2);

#job表更新
ALTER TABLE job MODIFY COLUMN spark_config VARCHAR(2000);
ALTER TABLE job MODIFY COLUMN source_config VARCHAR(4000);
ALTER TABLE job MODIFY COLUMN sink_config VARCHAR(4000);
ALTER TABLE job ADD COLUMN jvm_driver_config VARCHAR(1000) NULL;
ALTER TABLE job ADD COLUMN jvm_executor_config VARCHAR(1000) NULL;
ALTER TABLE job ADD COLUMN others_config VARCHAR(1000) NULL;
UPDATE job SET jvm_driver_config=substring_index(spark_config,",",1);
UPDATE job SET jvm_executor_config=substring_index(substring_index(spark_config,",",2),",",-1);
UPDATE job SET others_config=substring(substring_index(spark_config,substring_index(spark_config,",",2),-1),2);

#udf表更新
ALTER TABLE udf ADD COLUMN stream_type VARCHAR(100) NULL;
UPDATE udf SET stream_type='spark';
```

（2）执行【0.5.3-0.5.5 版本升级到 0.6.0 版本】更新要求
