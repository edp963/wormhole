---
layout: global
displayTitle: User Guide
title: User Guide
description: Wormhole WH_VERSION_SHORT User Guide page

---

* This will become a table of contents (this text will be scraped).
{:toc}
There are three user roles in Wormhole, namely Admin, User and App. The usage specification of User is introduced in this chapter. 

Normal users could log in and view Project authorized by Admin, have permissions to administrate Stream, Flow and Job and could use Namespace, UDF, etc. in Project.

## Stream Management

#### Type

Stream could process data of all types theoretically. In order to improve performance, data backup function of Hdfs and distribution function of Stream are optimized accordingly; therefore, Stream falls into three types: 

- default: writing data into systems like Kafka/RDBS/Elasticsearch/Hbase/Phoenix/Cassandra/MongoDB
- hdfslog: backing up data to Hdfs, providing data source for Job, implementing Kappa architecture
- routing: distributing data in a Topic to other Topics

![user-stream-type](/Users/swallow/IdeaProjects/wormhole/docs/img/user-stream-type.png)

#### Resource Configuration

-  Configuration of memory size of Driver and Executor and size of Vcore resource
-  Interval settings of time scheduling
-  Configuration of re-partitioning number (recommanded number is Executor's number)  after obtaining Kafka RDD
-  Configuration of maximum data volume obtained from Kafka Topic Partition per batch (in M) 

![user-stream-configs](/Users/swallow/IdeaProjects/wormhole/docs/img/user-stream-configs.png)

#### Topic Binding

Topic consumed by Stream are bound or cancelled automatically according to the start and stop of Flow. 

When Flow starts, corresponding Topic of its Source Namespace will be checked whether it has been bound to Stream. If it has been bound, it will not be registered repeatedly and will continue executing according to Offset consumed by Stream at present; if not, this topic will be bound to Stream, with current Offset set as initial Offset.

When Flow stops, corresponding Topic of its Source Namespace will be checked whether it is associated with other Flows in this Stream; if not, the topic will be unbound from this Stream. 

#### UDF Binding

When starts Stream, User could select UDF that needs to be loaded or cancel selected UDF. After Stream starts, User could click on "Renew" button and select UDF to be added; UDF could be loaded dynamically without restarting Stream. 

#### Start

- Select UDF.
- Configure consumption rate of each Topic (in item/s).
- Configure initial Offset consumed by each Topic Partition.

![user-stream-start](/Users/swallow/IdeaProjects/wormhole/docs/img/user-stream-start.png)

**Note:**

- When User starts Stream, if the resource it occupies is more than remaining available resource in this Project, it will fail to start with message of insufficient resource prompted. In this case, User could reduce the use of resource, stop other Flows or ask Admin to increase available computing resource in this Project.  
- When Stream starts, if it remains in a "starting" state or turns into a "failed" state, User could click on "Log" button, modify according to error message and restart Stream.

#### Renew

Hot deployment of UDF and dynamic ajustment of Offset and Rate consumed by Topic are supported during the running process of Stream.

**Note:**

If the item "configs" in Stream has been modified, User should restart Stream. 

#### State Transition

- New refers to Stream is created but does not start.
- Starting refers to Stream is starting.
- Waiting refers to Stream has been submitted to Yarn.
- Running refers to Stream is in a "running" state.
- Failed refers to Stream fails to start or fails at runtime.
- Stopping refers to Stream is stopping.
- Stopped refers to Stream has stopped.

Following is a state transition graph of Stream, in which "refresh" stands for "Refresh" button, "start" stands for "Start" button and "stop" stands for "Stop" button.

![user-stream-state-exchange](/Users/swallow/IdeaProjects/wormhole/docs/img/user-stream-state-exchange.png)

## Flow Management

In order to manage Flow, it is necessary to assign Stream, Source Namespace, Protocol and Sink Namespace, and configure data transformation logic.

#### Protocol

- "increment" refers to data only processing data_increment_data protocol.
- "initial" refers to data only processing data_initial_data protocol.
- "all" refers to data processing above two protocols.

#### Source Namespace

- If Wormhole isn't integrated with Dbus, only Kafka could work as source data system.
- If Wormhole has been integrated with Dbus, source data system configured in Dbus could be selected.
- There are some right controls for the authorized Namespaces: Source Namespace is the intersection of Namespaces in Kafka Instance associated with Stream and Namespaces which is accessible in the Project where Flow exists; Sink Namespace is Namespaces which is accessible in the Project where Flow exists without those synchronized from Dbus system.

#### Sink Namespace

Physical table corresponding to Sink Namespace should be created in advance. It is decided according to the following policies whether it is necessary to create UMS system fields, like `ums_id_(long type), ums_ts_(datetime type), ums_active_(int type)`, in the Schema of table:

- The source data is UMS type: three fields should be added in Sink table.
- The source data is UMS_Extension type: if `ums_ts_` field is configured in the Schema of source data, it should be also added in Sink table; if `ums_ts_, ums_id_` fields are configured in the Schema of source data, they should be also added in Sink table; if `ums_id_, ums_ts_, ums_op_` fields are configured in the Schema of source data,  `ums_id_, ums_ts_, ums_active_` should be added in Sink table. (Note: if just  `ums_ts_` field is configured, only "insert only" could be selected as long as writing data into Sink table)

#### Result Fields

 During the configuration process of result fields, "All" refers to the output of all fields; click on "Selected" to configure field names that needs to be output, which should be separated by comma if there are more than one.

#### Sink Config

- The configuration of Sink Config is related to system type. A configuration item sample of corresponding system will be shown at the top of the page after the "Config" button is clicked.  
- The value of "mutation_type" includes "i" and "iud", respectively standing for "insert only" and "insert, update and delete" when you write data into Sink table. If the value is "iud", `ums_id_(long), ums_ts_(datetime), ums_op_(string)` should exist in source data and `ums_id_(long), ums_ts_(datetime), ums_active_(int)` in Sink table. If the "mutation_type" isn't configured, its default value is "iud".  

#### Transformation

SQL and Custom Class are supported while data transformation logic is configured; more than one transformation logic could be configured, of which the sequence is adjustable. 

##### Custom Class

- Add wormhole/sparkxinterface dependency in pom.

  ```
  <dependency>
     <groupId>edp.wormhole</groupId>
     <artifactId>wormhole-sparkxinterface</artifactId>
     <version>0.4.0</version>
  </dependency>
  ```

- Inherit and implement interface of edp.wormhole.sparkxinterface.swifts.SwiftsInterface in wormhole/common/sparkxinterface module; you can refer to edp.wormhole.swifts.custom.CustomTemplate class in wormhole/sparkx module. 

- Compile and package, and put Jar package under the directory of $SPARK_HOME/jars.

- As for page configuration, select Custom Class and input full path of method name, like edp.wormhole.swifts.custom.CustomTemplate.

##### SQL

###### Lookup SQL

You can use Lookup SQL to associate data in other systems beyond the stream, such as RDBMS/Hbase/Redis/Elasticsearch. Rules are as follows. 

If Source Namespace is kafka.edp_kafka.udftest.udftable, Lookup Table belongs to RDBMS, such as eurususer table in database mysql.er_mysql.eurus_test, associated fields of Left Join are id, name, and id, name selected from Lookup table are the same as field names in kafka.edp_kafka.udftest.udftable in the Stream, the SQL sentence should be:  

```
select id as id1, name as name1, address, age from eurus_user where (id, name) in (kafka.edp_kafka.udftest.udftable.id, kafka.edp_kafka.udftest.udftable.name);
```

If Source Namespace is kafka.edp_kafka.udftest.udftable, Union Table is eurus_user table in database mysql.ermysql.eurustest, and eurus_user table has to contain UMS system fields which are identical with those of source data, the rules of SQL sentence should be the same as above. 

**Note:**

- In Lookup SQL, only one SQL could contain "group by" field, which should be also contained in "where in" conditional sentence. 
- Fields in "where" conditional sentence should be selected.
- If the selected field name is a duplicate of that of Flow Source Namespace, it should be renamed while selected.
- Union operation is based on the current data of main stream; if the searched data does not contain some fields in the main stream, "null" will be written automatically; if it contains some fields that the main stream does not contain, it will be removed automatically. 
- Only one SQL sentence could be written in each configuration box. 

###### Spark SQL

Spark SQL is used to process Source Namespace data, with table name directly added after "from". UDF method, used in this Flow and contained in corresponding Stream, could be used in Spark SQL.

###### Stream Join SQL

- Select other Source Namespace which needs to be associated; it can associate more than one Source Namespace.
- During the processing of Stream Join SQL,  data that fails to associate will be saved to HDFS; data retention time refers to the validity duration of data.
- The sentence rules of "select" is the same as that of Spark SQL.

#### Modify Flow

When modify Flow, you cannot modify selected Stream, SourceNamespace and SinkNamespace, but you can modify Protocol type, Result Fields, Sink Config and Transformation logic.

#### Start Flow

- When Sink data system is RDBMS, corresponding jdbc connector jar should be placed under the directory of $SPARK_HOME/jars, and start or restart Stream.
- When you click on "Start" button, the information of Flow will be committed to Stream in the background, and the Topic where Flow Source Namespace and Stream Join Namespaces fall will be bound to Stream. 
- Stream receives and parses the information of Flow. If it is parsed successfully, success message will be returned and the state of Flow will turn from "starting" to "running"; if the parse fails, the state of Flow will turn from "starting" to "failed". You can view Driver log from Yarn and reconfigure Flow according to errors.

#### Renew Flow

- During the operation process of Flow, you can modify the logic of Flow. Click on "Renew" button and update the configuration of Stream dynamically.
- If the configuration of Namespaces/Databases/Instances used in Flow is modified, you should click on "Renew" button and update the configuration of Stream dynamically.
- The state of Flow converts to "updating" after "Renew" button is clicked and to "running" or "failed" after receiving the feedback information from Stream. 

#### Stop Flow

When you stop Flow, Stream will receive a cancelling command, and it will be checked whether the Topics associated with other Flows in the Stream contain the one associated with this Flow; if not, this Topic will be unbound from Stream. 

#### State Transition of Flow

- New refers to Flow is created does not start.
- Starting refers to Flow is starting.
- Running refers to Flow is in a "running" state.
- Suspending refers to Flow is in a "suspended" state. When Stream is not in a "running" state, the state of Flow turns from "starting", "running" or "updating" to "suspending"; When Stream is in a "running" state, the state of Flow converts to "running" or "failed" automatically.
- Failed refers to Flow fails to start.
- Stopping refers to Flow is stopping.
- Stopped refers to Flow has stopped.

### Job

You can implement Lambda or Kappa architecture easily with the help of Job.

Back up source data to Hdfs by using hdfslog Stream firstly. You can configure Job to retry in case of a Flow error or when it needs recalculating. Refer to Stream section and Flow section for specific configuration. 