---
layout: global
displayTitle: Concept
title: Concept
description: Wormhole WH_VERSION_SHORT Concept page
---

* This will become a table of contents (this text will be scraped).
{:toc}
## Namespace

Namespace, composed of seven parts, is a unique specification to locate data table on data system defined by Wormhole.

#### [Datastore].[Datastore Instance].[Database].[Table].[Table Version].[Database Partition].[Table Partition]

- Datastore refers to the type of data storage system, such as Oracle, Mysql, Hbase, Elasticsearch, Kafka and so on.
- Datastore Instance refers to the alias name of physical address of data storage system.
- Database refers to database in RDBS, namespace in Hbase, index in Elasticsearch or topic in Kafka.
- Table refers to data table in RDBS, data table in Hbase, file in Elasticsearch or data <u>under</u> a topic in Kafka.
- Table Version refers to the version of table structure, and normally the value of Table Version is incremental with the change of table structure. (At present "*" is used to match data of all versions during processing in Wormhole)
- Database Partition refers to partition name of Database. (At present "*" is used to match data of all partitions during processing in Wormhole)
- Table Partition refers to partition name of table. (At present "*" is used to match data of all table partitions during processing in Wormhole)

eg: mysql.test.social.user.*.*.*    kafka.test.social.user.*.*.*

## UMS (Unified Messaging Specification)

UMS is messaging specification (JSON format) defined by Wormhole. UMS is used to abstract and unify all the structured message and carry out parallel processing on several logical DAG in single physical DAG through contained Schema information of structured data. Meanwhile, it avoids operation to synchronize metadata with external data systems.

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

#### protocol (message protocol)

- data_increment_data refers to incremental data.
- data_initial_data refers to initial data.
- data_increment_heartbeat refers to incremental heartbeat data.
- data_increment_termination refers to the termination of incremental data.

#### schema (message scource and table structure information)

Namespace is used to assign message source and fields are used to assign table structure information. `ums_id_,ums_ts_,ums_op_` are three indispensable system fields, with which Wormhole implements the logic to write data in an idempotent way.

- ums_id_: long, a type refers to a unique identification of message, of which the value should be incremental according to the generation sequence of message.
- ums_ts_: datetime, a type refers to the generation time of each message.
- ums_op_: string, a type assigns the generation method of each message, of which the value is "i", "u" or "d", namely insertion, update and deletion.

#### payload (data to record information)

- tuple: a tuple corresponds to a message.

## UMS_Extension (extended format of UMS) 

Wormhole also supports UMS_Extension except UMS. User could customize data format, including nested struture. Key of message in Kafka should be set as data_increment_data.sourceNamespace during usage, and a data sample should be pasted on Wormhole page to configure simply. For example, if sourceNamespace is `kafka.kafka01.datatopic.user.*.*.*`, key of message in Kafka should be `data_increment_data.kafka.kafka01.datatopic.user.*.*.*`.

**If messages of a sourceNamespace need to be allocated to more than one partition, the key of message could be set as `data_increment_data.kafka.kafka01.datatopic.user.*.*.*...abc or data_increment_data.kafka.kafka01.datatopic.user.*.*.*...bcd`, that is,  "…" and random number or any characters are added after sourceNamespace.**

## Stream

Stream is a computing framework encapsulating Spark Streaming and consumes Kafka as data source. As the stream computing engine of Wormhole, its key for matching messages, sourceNamespace and corresponding processing logic could write data into multiple data systems in an idempotent way. During processing, Stream would provide feedback of error messages, heartbeat messages, processing of data amount and latency information. 

A Stream could process multiple Flow, which shares computing resource.  

## Flow

Flow emphasizes on data's sourceNamespace, sinkNamespace and its processing logic.

Flow supports configuration with SQL, UDF customization and Class customization, and could associate data of other systems like RDBS, Hbase, Phoen  ix, Redis and Es。

After Flow is configured and logs in Stream, Stream receives Flow instruction and processes data according to its sourceNamespace, sinkNamespace and business logic.

## Job 

Job, of which the data source is Hdfs, is similar to Spark Job.

Data in Kafka has a certain life cycle, but it could be backed into Hdfs through Stream. If it is necessary to recompute data at some time node or supplement data in some data slot, backup data in Hdfs could be read, configured with the same processing logic as Flow, and written into target table through Job.