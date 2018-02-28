---
ilayout: global
displayTitle: Wormhole Overview
title: Overview
description: Wormhole WH_VERSION_SHORT documentation homepage
---
> A Stream Processing Platform from [Technology R&D Center](http://crdc.creditease.cn/) of [CreditEase](http://english.creditease.cn/) 

**Wormhole is a one-stop stream processing cloud platform solution （SPaaS - Stream Processing as a Service）.**

Wormhole is geared to the needs of those who develop, manage and operate the big data stream processing project. It is aimed at providing a unified and abstract conceptual system, a visual operation interface, simple and smooth configuration management process and business logic development based on SQL. Meanwhile, Wormhole has masked the <u>underlying</u> technical details and dramatically lowered the threshold for those who manage and operate data project, making the development, management and operation more lightweight, agile, controllable and reliable.

## Architecture

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/wh4_pipeline_overview.png" alt="" width="600"/>

### Design Philosophy

- **Unified DAG High-order Fractal <u>Abstraction</u>**
  - Creating <u>physical DAG</u> composed of Source DataSys, Kafka Topic, Spark Stream (Flink Stream) and Sink DataSys
  - It is possible to carry out parallel processing on more than one logical DAG composed of Source Namespace, Flow and Sink Namespace in each physical DAG.
  - Each Flow is typical Spark RDD DAG in itself.
- **Unified and General <u>Stream Message UMS Protocol</u> Abstraction **
  - UMS is stream message protocol specification defined by Wormhole.
  - UMS is used to abstract and unify all the structured message.
  - Schema information of structured data is contained in UMS.
  - Wh4 supports user-defined semi-structured JSON format.
- **Unified Namespace Abstraction <u>in Data Logical Table</u>**
  - Namespace is used to uniquely locate all the data storage <u>and</u> structured logical table.
  - [Data System].[Instance].[Database].[Table].[Table Version].[Database Partition].[Table Partition]

### Key Features

- **It supports the development and implementation of stream pcocessing project in a visualized and configurable way <u>with SQL</u>.**
- **It supports the management, operation, diagnosis and monitoring of imperative and dynamic stream processing.**
- **It supports unified structured UMS message and customized semi-structured JSON message.**
- **It supports three kinds of processing, namely the insertion, deletion and update of event message stream.**
- **It supports simultaneous parallel processing of more than one <u>logical business streams</u> <u>in</u> single physical stream.**
- **It supports lookup anywhere and pushdown anywhere during stream data processing.**
- **It supports stream processing of event timestamp based on business strategy.**
- **It supports the registration management and dynamic loading of UDF.**
- **It supports the writing of multi-target data system into database in a parallel and idempotent manner.**
- **It supports multi-level data quality management based on incremental messages.**
- **It supports stream processing and batch processing based on incremental messages.**
- **It supports Lambda architecture and Kappa architecture.**
- **It supports seamless integration with three-party system and acts as  flow-control engine of three-party system.**
- **It supports private cloud deployment, security right control and multi-tenant resource management.**

## Experience

#### Admin could create Project/Namespace/User/UDF and <u>view</u> all the Flow/Stream/Job.
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_1.png" alt="" width="600"/>

#### Admin could allocate Namespace resource, User resource, UDF resource and computing resource to Project, so as to support multi-tenant resource isolation.
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_2.png" alt="" width="600"/>

#### User with authority could carry out development, implementation, management and operation on the Project <u>which is accessible</u>. 

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_1_project.png" alt="" width="600"/>

#### User could create stream job pipeline (flow) through some simple configuration steps and the knowledge of the <u>sourceNamespace</u>, <u>sinkNamespace</u> and transformation methods of data.
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_1.png" alt="" width="600"/>

#### User could perform transformation in most common scenarios of jobs during stream data processing, and processing logic could be implemented in most work through SQL configuration.
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_2.png" alt="" width="600"/>

#### The concepts of Flow and Stream could be recognized by Wormhole, which supports User utilizing computing resouce more accurately and flexibly through the parallel processing of several logical Flow <u>in</u> a physical Stream (corresponding to a Spark Stream). Meanwhile, User could configure and adjust parameters accurately to better balance requirements and resources. 

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_3_stream.png" alt="" width="600"/>

#### Wormhole also supports batch processing of Job, implementing processing logic through configuration and writing into more than one isomerous sink. The combination of Flow and Job makes it easy to implement Lambda architecture and Kappa architecture.
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_1.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_2.png" alt="" width="600"/>

#### User could view Namespace/User/UDF/Resource related to the Project.
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_ns.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_user.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_6_udf.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_7_res.png" alt="" width="600"/>

#### User could monitor the throughput and latency of all the running Flow/Stream in the Project.
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_8_mon.png" alt="" width="600"/>

#### The above is brief preview of functions and user experiences of Wormhole. Please see other sections for more enhanced detail functions.


## Latest Release

Please download the latest [RELEASE](https://github.com/edp963/wormhole/releases/download/0.3.0/wormhole-0.3.0.tar.gz).

## Get Help

- **Mailing list**: edp_support@groups.163.com
- **WeChat**: edpstack <img src="https://github.com/edp963/edp-resource/raw/master/WeChat.jpg" alt="" width="100"/>

## License

Wormhole is under the Apache 2.0 license. See the [LICENSE](https://github.com/edp963/wormhole/blob/master/LICENSE) file for details.

 