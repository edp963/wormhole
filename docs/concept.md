---
layout: global
displayTitle: Concept
title: Concept
description: Wormhole Concept page
---

* This will become a table of contents (this text will be scraped).
{:toc}
## Namespace（命名空间）

Namespace是Wormhole定义的唯一定位数据系统上数据表的规范，由七部分组成。

#### [Datastore].[Datastore Instance].[Database].[Table].[Table Version].[Database Partition].[Table Partition]

- Datastore 代表数据存储系统类型，如 Oracle，Mysql，Hbase，Elasticsearch，Kafka 等
- Datastore Instance 代表数据存储系统物理地址别名
- Database 代表 RDBS 中的数据库，Hbase 中的命名空间，Elasticsearch 中的索引，Kafka 中的主题
- Table 代表 RDBS 中的数据表，Hbase 中的数据表，Elasticsearch 中的文档，Kafka 中的某主题下的数据
- Table Version 代表Table 的表结构版本，一般情况下 Table Version 的值应随表结构的变化递增（目前Wormhole 中处理时用“*”匹配所有版本的数据）
- Database Partition 代表 Database 的分区名称（目前 Wormhole 中处理时用“*”匹配所有分区的数据）
- Table Partition 代表 Table 的分表名称（目前 Wormhole 中处理时用“*”匹配所有分表的数据）

例如：`mysql.test.social.user.*.*.*    kafka.test.social.user.*.*.*`

## UMS（统一消息规范）

UMS 是 Wormhole 定义的消息规范（JSON 格式），UMS 试图抽象统一所有结构化消息，通过自身携带的结构化数据 Schema 信息，来实现一个物理 DAG 同时处理多个逻辑 DAG 的能力，也避免了和外部数据系统同步元数据的操作。

```
  {
  "protocol": {
    "type": "data_increment_data"          
  },
  "schema": {
    "namespace": "kafka.kafka01.datatopic.user.*.*.*",
    "fields": [
      {
        "name": "ums_id_",
        "type": "long",
        "nullable": false
      },
      {
        "name": "ums_ts_",
        "type": "datetime",
        "nullable": false
      },
      {
        "name": "ums_op_",
        "type": "string",
        "nullable": false
      },
      {
        "name": "key",
        "type": "int",
        "nullable": false
      },
      {
        "name": "value1",
        "type": "string",
        "nullable": true
      },
      {
        "name": "value2",
        "type": "long",
        "nullable": false
      }
    ]
  },
  "payload": [
    {
      "tuple": [ "1", "2016-04-11 12:23:34.345123", "i", "23", "aa", "45888" ]
    },
    {
      "tuple": [ "2", "2016-04-11 15:23:34.345123", "u", "33", null, "43222" ]
    },
    {
      "tuple": [ "3", "2016-04-11 16:23:34.345123", "d", "53", "cc", "73897" ]
    }
  ]
}
```

#### protocol 代表消息协议

- data_increment_data 代表增量数据
- data_initial_data 代表全量数据
- data_increment_heartbeat 代表增量心跳数据
- data_increment_termination 代表增量数据结束

#### schema 代表消息来源及表结构信息

namespace 指定消息的来源，fields 指定表结构信息，其中 `ums_id_,ums_ts_,ums_op_` 三个字段是必需的三个系统字段，Wormhole 使用这三个字段实现幂等写数据的逻辑。

- ums_id_: long 类型，用来唯一标识消息，须根据消息生成的顺序递增
- ums_ts_: datetime 类型，每一条消息产生的时间
- ums_op_: string 类型，指定每条消息生成的方式，值为 "i" 或 "u" 或 "d" ，分别代表新增，更新，删除操作

#### payload 代表数据本身

- tuple：一个tuple对应一条消息

## UMS_Extension (UMS 扩展格式) 

除 UMS 格式外，Wormhole 支持 UMS_Extension（UMS扩展格式），用户可自定义数据格式，且支持嵌套结构。使用时须将 Kafka 消息的 key 设置为 data_increment_data.sourceNamespace，然后在 Wormhole 页面上粘贴数据样式简单配置即可。如 sourceNamespace 为 `kafka.kafka01.datatopic.user.*.*.*`，则 Kafka 消息的 key 须为 `data_increment_data.kafka.kafka01.datatopic.user.*.*.*`。

**若一个 sourceNamespace 的消息需要随机分配到多个 partition，消息的 key 可设置为`data_increment_data.kafka.kafka01.datatopic.user.*.*.*...abc 或 data_increment_data.kafka.kafka01.datatopic.user.*.*.*...bcd`，即在 sourceNamespace 后面添加“…”，之后可添加随机数或任意字符。**

**若 UMS_Extension 类型数据有增删改操作且需要幂等写入，也须配置 `ums_id_,ums_ts_,ums_op_` 三个字段。具体配置方式请参考 Admin-Guide Namespace章节。**

## Stream

Stream 是在 Spark Streaming 上封装的一层计算框架，消费的数据源是 Kafka。Stream 作为 Wormhole 的流式计算引擎，匹配消息的 key，sourceNamespace 和其对应处理逻辑，可将数据以幂等的方式写入多种数据系统中。处理过程中 Stream 会反馈错误信息、心跳信息、处理数据量及延时等信息。

一个 Stream 可以处理多个 Namespace 及其处理逻辑，共享计算资源。

## Flow

Flow 关注的是数据从哪来（sourceNamespace），到哪去（sinkNamespace），及中间的处理逻辑。

Flow 支持 SQL 配置，自定义UDF，自定义 Class，且可以关联其他 RDBS/Hbase/Phoenix/Redis/Es 等系统中的数据。

Flow 配置好后可以注册到 Stream，Stream 接收 Flow 指令后，根据指令中的 sourceNamespace，sinkNamespace 及业务逻辑处理数据。

## Job 

Job 相当于 Spark Job，其数据源是 HdfsLog Stream 备份在 Hdfs 上的数据。Stream/Flow/Job 组合可实现 Lambda 架构和 Kappa 架构。

Kafka 中数据有一定的生命周期，可通过 Stream 将 Kafka 中数据备份到 Hdfs 上。后续需要从某个时间节点重新计算或者补充某个时间段的数据，可通过 Job 读取 Hdfs上 的备份数据，配置与 Flow 相同的处理逻辑，将数据写入目标表。

**注意：目前 UMS_Extension 类型数据只支持通过 Stream 将 Kafak 中数据备份到 Hdfs 上，Job 还不支持读取 UMS_Extension 类型数据。**