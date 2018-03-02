---
layout: global
displayTitle: Deployment
title: Deployment
description: Wormhole WH_VERSION_SHORT Deployment page 
---

* This will become a table of contents (this text will be scraped).
{:toc}

## Preparation

#### Environment Preparation
- JDK1.8
- Hadoop-client (HDFS, YARN) (supporting version 2.6+)
- Spark-client (supporting version 2.1.1, 2.2.0)

#### Dependent Service

- Hadoop Cluster (HDFS, YARN) (supporting version 2.6+)
- Zookeeper
- Kafka (supporting version 0.10.0.0)
- Elasticsearch (supporting version 5.x) (unnecessay option, but the throughput and latency of data processing by wormhole cannot be viewed without it)
- Grafana (supporting version 4.x) (unnecessay option, but the graphical depiction of throughput and latency of data processing by wormhole cannot be viewed without it)
- MySQL

#### Preparation of Jar
mysql-connector-java-{your-db-version}.jar


## Deployment and Configuration

**Download wormhole-0.4.0.tar.gz (Link: https://pan.baidu.com/s/1pKAqp31 Password: 4azr), or you can compile it by yourself.**

```
wget https://github.com/edp963/wormhole/releases/download/0.4.0/wormhole-0.4.0.tar.gz
tar -xvf wormhole-0.4.0.tar.gz
or you can compile it by yourself, and the generated tar is in wormhole/target
git clone -b 0.4 https://github.com/edp963/wormhole.git
cd wormhole
mvn install package -Pwormhole
```

**Configure environment variable of WORMHOLE_HOME.**

**Modify configuration files of application.conf.**

```
Introdution of conf/application.conf configuration item

wormholeServer {
  host = "127.0.0.1"
  port = 8989
  token.timeout = 7
  request.timeout = 120s
  admin.username = "admin"    #default admin user name
  admin.password = "admin"    #default admin user password
}

mysql = {
  driver = "slick.driver.MySQLDriver$"
  db = {
    driver = "com.mysql.jdbc.Driver"
    user = "root"
    url = "jdbc:mysql://localhost:3306/wormhole"
    password = "*******"
    numThreads = 4
  }
}

spark = {
  wormholeServer.user = "wormhole"   #WormholeServer linux user
  wormholeServer.ssh.port = 22       #ssh port, please set WormholeServer linux user can password-less login itself remote
  spark.home = "/usr/local/spark"
  yarn.queue.name = "default"        #WormholeServer submit spark streaming/job queue
  wormhole.hdfs.root.path = "hdfs://nn1/wormhole"   #WormholeServer hdfslog data default hdfs root path
  yarn.rm1.http.url = "localhost:8088"    #Yarn ActiveResourceManager address
  yarn.rm2.http.url = "localhost2:8088"   #Yarn StandbyResourceManager address
}

zookeeper.connection.url = "localhost:2181"  #WormholeServer stream and flow interaction channel

kafka = {
  brokers.url = "locahost:9092"         #WormholeServer feedback data store
  zookeeper.url = "localhost:2181"
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
    max.poll.records = 500
    request.timeout.ms = 80000
    max.partition.fetch.bytes = 10485760
  }
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

#Dbus integration, if not set, please comment it
#dbus.namespace.rest.api.url = ["http://localhost:8080/webservice/tables/riderSearch"]
```
**Set the database code of wormhole server mysql as uft8, and authorize it as remotely accessible.**

**Upload mysql-connector-java-{version}.jar to catalogue $WORMHOLE_HOME/lib.**

**Start service with corresponding user of spark.wormholeServer.user in application.conf, and set this user to log in remotely without code through ssh. **

**Grafana should be configured to be logged in without code by users who can use viewer type, with token of admin type generated and configured in grafana.admin.token in $WORMHOLE_HOME/conf/application.conf**

**Switch to root user, and authorize activate user of WormholeServer to read and write HDFS catalogue. If it fails, please authorize manually according to the tips.**

```
#change hadoop to super-usergroup
./deploy.sh --hdfs-super-usergroup=hadoop corresponding to Hadoop cluster
```

## Start and Stop

#### Start

```
./start.sh

Table, kafka topic, elasticsearch index and grafana datasource will be created automatically while wormhole is started. But it would be failed sometimes because of system environment, so it is necessary to create it manually.

topic name: wormhole_feedback partitions: 4
topic name：wormhole_heartbeat partitions: 1

# Create or modify topic command
./kafka-topics.sh --zookeeper localhost:2181 --create --topic wormhole_feedback --replication-factor 1 --partitions 4
./kafka-topics.sh --zookeeper localhost:2181 --create --topic wormhole_heartbeat --replication-factor 1 --partitions 1

./kafka-topics.sh --zookeeper localhost:2181 --alter --topic wormhole_feedback  --partitions 4
./kafka-topics.sh --zookeeper localhost:2181 --alter --topic wormhole_heartbeat  --partitions 1
```

#### Stop

```
./stop.sh
```

#### Restart

```
./restart.sh
```

**Get access to http://ip:port and you can experiment with Wormhole. You are possible to log in as admin, of which the default username is admin and the password can be found in the configuration of application.conf.**