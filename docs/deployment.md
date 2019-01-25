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
- Spark-client （支持版本 2.2.0，2.2.1）(若使用Spark Streaming引擎，须部署Spark-client)
- Flink-client （支持版本 1.5.1）(若使用Flink引擎，须部署Flink-client)

#### 依赖服务

- Hadoop 集群（HDFS，YARN）（支持版本 2.6+）
- Zookeeper
- Kafka （支持版本 0.10.2.2）
- Elasticsearch（支持版本 5.x）（非必须，若无可通过在配置文件中配置mysql查看wormhole 处理数据的吞吐和延时）
- MySQL

#### Jar包准备

mysql-connector-java-{your-db-version}.jar

**注意：升级至0.6.0版本，须将Kafka版本由0.10.0.0升级至0.10.2.2，0.10.2.2以上版本须自行测试**

## 部署配置

#### 下载安装包

**下载 wormhole-0.6.0.tar.gz 包 (链接：https://pan.baidu.com/s/1CYu39S-3TcWTJsRDXqFuHw  提取码：oo2o )，或者自编译**

```
下载wormhole-0.6.0.tar.gz安装包
tar -xvf wormhole-0.6.0.tar.gz
或者自编译，生成的tar包在 wormhole/target
git clone -b 0.6 https://github.com/edp963/wormhole.git
cd wormhole
mvn install package -Pwormhole
```

#### 配置环境变量

**配置 SPARK_HOME/HADOOP_HOME 环境变量**

#### 修改配置文件

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
  wormholeServer.user = "wormhole"   #WormholeServer linux user
  wormholeServer.ssh.port = 22       #ssh port, please set WormholeServer linux user can password-less login itself remote
  spark.home = "/usr/local/spark"
  yarn.queue.name = "default"        #WormholeServer submit spark streaming/job queue
  wormhole.hdfs.root.path = "hdfs://nn1/wormhole"   #WormholeServer hdfslog data default hdfs root path
  yarn.rm1.http.url = "localhost:8088"    #Yarn ActiveResourceManager address
  yarn.rm2.http.url = "localhost:8088"   #Yarn StandbyResourceManager address
}

flink = {
  home = "/usr/local/flink"
  yarn.queue.name = "default"
  feedback.state.count=100
  checkpoint.enable=false
  checkpoint.interval=60000
  stateBackend="hdfs://nn1/flink-checkpoints"
  feedback.interval=30
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
    poll-interval = 20ms
    poll-timeout = 1s
    stop-timeout = 30s
    close-timeout = 20s
    commit-timeout = 70s
    wakeup-timeout = 60s
    max-wakeups = 10
    session.timeout.ms = 60000
    heartbeat.interval.ms = 50000
    max.poll.records = 1000
    request.timeout.ms = 80000
    max.partition.fetch.bytes = 10485760
  }
}

#kerberos = {
#  keyTab=""      #the keyTab will be used on yarn
#  spark.principal=""   #the principal of spark
#  spark.keyTab=""      #the keyTab of spark
#  server.config=""     #the path of krb5.conf
#  jaas.startShell.config="" #the path of jaas config file which should be used by start.sh
#  jaas.yarn.config=""     #the path of jaas config file which will be uploaded to yarn
#  server.enabled=false   #enable wormhole connect to Kerberized cluster
#}

# choose monitor method among ES、MYSQL
monitor ={
   database.type="ES"
}

#Wormhole feedback data store, if doesn't want to config, you will not see wormhole processing delay and throughput
#if not set, please comment it

#elasticSearch.http = {
#  url = "http://localhost:9200"
#  user = ""
#  password = ""
#}

#display wormhole processing delay and throughput data, get admin user token from grafana
#garfana should set to be anonymous login, so you can access the dashboard through wormhole directly
#if not set, please comment it

#grafana = {
#  url = "http://localhost:3000"
#  admin.token = "jihefouglokoj"
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

#### Flink CheckPoint配置

wormhole 0.6及之后版本支持flink checkpoint配置。

如果flink.checkpoint.enable=false则不使用checkpoint，默认为不适用。

如果使用checkpoint则需要配置flink.checkpoint.enable=true，另外还可以设置checkpoint的间隔时间和存储系统。通过flink.checkpoint.interval可设置checkpoint的间隔时间，默认为60000ms。通过flink.stateBackend可设置checkpoint的存储位置。

#### Feedback State存储位置配置

wormhole在0.6版本之前的feedback state默认存储在ES中，在0.6版本之后，将支持用户根据需求在ES与MySQL中间选择合适的存储库进行数据存储。如果需要将存储位置由ES迁往MySQL，可以参照下面的步骤进行配置。通过配置monitor.database.type选择存储位置

`monitor.database.type="MYSQL" #存储到mysql中`

`monitor.database.type="ES" #存储到ES中`

当选择存储到mysql时，需要在wormhole/rider/conf/wormhole.sql新建feedback_flow_stats表，并在wormhole配置的数据库中执行该文件，从而在数据库中建立feedback_flow_stats表

#### Wormhole集群部署

**部署说明**

wormhole 0.5.5-beta及之后版本支持多套wormhole隔离部署

若只部署一套Wormhole可跳过此步骤

为支持同一hadoop集群环境中部署多套Wormhole，在配置文件conf/application.conf中增加了wormholeServer.cluster.id参数（要求唯一）。单套Wormhole部署不设置wormholeServer.cluster.id或者wormholeServer.cluster.id=""。为兼容之前版本，可不设置该变量。**注意：之前版本不要随意增加该参数，否则无法读取对应的zookeeper和hdfs信息，无法正常运行已配置的stream和flow，即之前版本可以保持不变，新部署的Wormhole增加该参数即可。**

##### 单套Wormhole部署

- 单套Wormhole部署只需将wormholeServer.cluster.id设置为空或者不进行设置即可

  **说明**

- Kafka feedback topic：为kafka.consumer.feedback.topic
- ES feedback index：为elasticSearch.wormhole.feedback.index
- HDFS路径为：spark.wormhole.hdfs.root.path
- Zookeeper路径为：zookeeper.wormhole.root.path/cluster.id

##### 多套Wormhole隔离部署

- wormholeServer.cluster.id（必须配置）：每套Wormhole唯一的uuid，不可重复
- kafka.using.cluster.suffix（选择设置）：该变量标记是否将wormholeServer.cluster.id作用于kafka.consumer.feedback.topic。如果kafka.using.cluster.suffix=false，则feedback topic为kafka.consumer.feedback.topic；如果kafka.using.cluster.suffix=true，则feedback topic为kafka.consumer.feedback.topic + "_" + cluster.id
- elasticSearch.wormhole.using.cluster.suffix（选择设置）：该变量标记是否将wormholeServer.cluster.id作用于elasticSearch.wormhole.feedback.index。如果elasticSearch.wormhole.using.cluster.suffix=false，则feedback index为elasticSearch.wormhole.feedback.index ；如果elasticSearch.wormhole.using.cluster.suffix=true，则feedback index为elasticSearch.wormhole.feedback.index + "_" + cluster.id

  **说明**

- MySQL：与cluster.id是否存在无关，所以部署多集群时，只要用不同的库的url即可
- Kafka feedback topic：参考上文kafka.using.cluster.suffix的设置
- ES feedback index：参考上文elasticSearch.wormhole.using.cluster.suffix的设置
- HDFS：spark.wormhole.hdfs.root.path为一级根目录名，HDFS路径为spark.wormhole.hdfs.root.path/cluster.id
- Zookeeper：zookeeper.wormhole.root.path为一级根目录名，Zookeeper路径为zookeeper.wormhole.root.path/cluster.id

#### Wormhole接入Kerberos支持

wormhole 0.6及之后版本支持接入kerberos支持。若无需接入KerBeros支持，可跳过此步骤

##### Spark中kerberos认证

Spark只有在集群模式(即--master yarn或者--master yarn-cluster)下，才会支持kerberos认证。Spark通过向yarn集群提交任务时设定相应的参数支持kerberos认证，需要指定的参数包括—principal、--keytab、--files、--conf。这些配置都可以通过配置wormhole的application.conf中的对应项来完成

##### Flink中kerberos认证

与spark不同，flink在配置文件中实现对kerberos认证支持，仅需修改flink/conf/flink-conf.yaml文件，即可开启flink应用与kerberos集群的对接。flink-conf.yaml具体配置为：

```
security.kerberos.login.keytab: keytab_path
security.kerberos.login.principal: principal_path
security.kerberos.login.contexts: client,kafkaClient
```

其中，security.kerberos.login.keytab与security.kerberos.login.principal分别对应的是kdc服务器生成的keytab和principal文件的路径。security.kerberos.login.contexts对应的是用户要对接的开启了kerberos认证的kafka集群与zookeeper集群

##### Wormhole中kerberos认证

目前版本的wormhole支持全部启用kerberos认证的安全hadoop集群环境和不启用kerberos认证的hadoop集群环境，不支持部分组件启用，部分组件不启用的场景

启用kerberos认证，需要在配置文件application.conf中对下列参数进行设置。参数及设置说明如下：

```
kerberos = {
  keyTab=""      #the keyTab will be used on yarn
  spark.principal=""   #the principal of spark
  spark.keyTab=""      #the keyTab of spark
  server.config=""     #the path of krb5.conf
  jaas.startShell.config="" #the path of jaas config file which should be used by start.sh
  jaas.yarn.config=""     #the path of jaas config file which will be uploaded to yarn
  server.enabled=false   #enable wormhole connect to Kerberized cluster
}
```

**特别说明：**

   keyTab对应的keyTab文件与jaas.yarn.config对应的jaas.conf文件中指定的keyTab文件为同一个keyTab文件；

   Spark.keyTab对应的keyTab文件与jaas.startShell.config对应的jaas.conf文件中指定的keyTab文件为同一个keyTab文件

上述提到的两个keyTab文件必须不同名，内容可以是相同的。否则，启动spark stream时，将会报错。导致这种情况发生的原因是，为了在yarn上读取安全的kafka集群，我们需要在spark-submit的files参数中上传yarn上使用的keytab，但是，spark-submit的keytab文件也会被上传，两者会发生冲突，进而导致程序无法正常启动

##### 注意事项

在kerberosized cluster集群模式下，所有kafka topic都被严格控制创建、访问、写入权限，因此，一旦开启kerberos认证，wormhole将不再支持在kafka集群中没有wormhole_feedback与wormhole_heartbeat这两个topic的情况下，自动创建这两个topic的操作。所以，需要用户联系kafka集群的管理人员，由他创建这两个topic。

目前，Wormhole仅支持kafka-0.10.2.2集群kerberos认证功能，其他版本kafka暂不支持kerberos认证。

#### 授权远程访问

设置 wormhole server mysql 数据库编码为 uft8，并授权可远程访问

上传 mysql-connector-java-{version}.jar 至 $WORMHOLE_HOME/lib 目录

须使用 application.conf 中 spark.wormholeServer.user 项对应的 Linux 用户启动服务，且须配置该 Linux 用户可通过 ssh 远程免密登录到自己

若配置 Grafana，Grafana 须配置可使用 viewer 类型用户匿名登陆，并生成 admin 类型的 token，配置在 $WORMHOLE_HOME/conf/application.conf 中grafana.admin.token 项中

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

#### 0.5.3-0.5.5版本升级到0.6.0版本

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

（2）停止所有flow

在0.6.0版本启动之前，需停止以前版本所有sparkx的flow（包括starting、running、suspending、updating状态的flow）, 并记录当前stream消费到的topic offset，重启stream时，手动设定从之前记录的offset消费

#### 0.5.0-0.5.2版本升级到0.6.0版本

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

（2）执行【0.5.3-0.5.5版本升级到0.6.0版本】更新要求

