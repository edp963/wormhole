---
layout: global
displayTitle: Quick Start
title: Quick Start
description: Wormhole WH_VERSION_SHORT Quick Start page
---

本章节以一个流式项目的实施示例介绍Wormhole页面的使用流程。Wormhole支持的数据源系统为Kafka，可集成Dbus抽取Mysql数据库的全量及增量数据。

业务需求：实时处理kafka中数据，处理过程中关联Mysql数据库某表，然后过滤或转换数据，写入Hbase系统中。

Kafka源数据需设置key

Wormhole面向大数据流式处理项目的开发管理运维人员，致力于提供统一抽象的概念体系，直观可视化的操作界面，简单流畅的配置管理流程，基于SQL即可完成的业务逻辑开发方式，并且屏蔽了流式处理的底层技术细节，极大的降低了数据项目管理运维门槛，使得大数据流式处理项目的开发管理运维变得更加轻量敏捷可控可靠。

## Architecture

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/wh4_pipeline_overview.png" alt="" width="600"/>

### 设计理念

- **统一DAG高阶分形抽象**
  - 构建由Source DataSys，Kafka Topic，Spark Stream（Flink Stream），Sink DataSys组成的物理DAG
  - 每个物理DAG里可以并行处理多个由Source Namespace，Flow，Sink Namespace组成的逻辑DAG
  - 每个Flow本身是典型的Spark RDD DAG
- **统一通用流消息UMS协议抽象**
  - UMS是Wormhole定义的流消息协议规范
  - UMS试图抽象统一所有结构化消息
  - UMS自身携带结构化数据Schema信息
  - Wh4支持用户自定义半结构化JSON格式
- **统一数据逻辑表命名空间Namespace抽象**
  - Namespace唯一定位所有数据存储所有结构化逻辑表
  - [Data System].[Instance].[Database].[Table].[Table Version].[Database Partition].[Table Partition]

### 主要特性

- **支持可视化，配置化，SQL化开发实施流式项目**
- **支持指令式动态流式处理的管理，运维，诊断和监控**
- **支持统一结构化UMS消息和自定义半结构化JSON消息**
- **支持处理增删改三态事件消息流**
- **支持单个物理流同时并行处理多个逻辑业务流**
- **支持流上Lookup Anywhere，Pushdown Anywhere**
- **支持基于业务策略的事件时间戳流式处理**
- **支持UDF的注册管理和动态加载**
- **支持多目标数据系统的并发幂等入库**
- **支持多级基于增量消息的数据质量管理**
- **支持基于增量消息的流式处理和批量处理**
- **支持Lambda架构和Kappa架构**
- **支持与三方系统无缝集成，可作为三方系统的流控引擎**
- **支持私有云部署，安全权限管控和多租户资源管理**

## Experience

#### Admin可以创建Project/Namespace/User/UDF，并且可以查看所有Flow/Stream/Job
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_1.png" alt="" width="600"/>

#### Admin可以为Project分配Namespace资源/User资源/UDF资源/计算资源，以支持多租户资源隔离
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_2.png" alt="" width="600"/>

#### User可以对自己有权限的Project进行开发实施和管理运维工作
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_1_project.png" alt="" width="600"/>

#### User可以通过简单配置步骤即可搭建起一个流式作业pipeline（Flow），只需关注数据从哪来到哪去和如何转换处理
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_1.png" alt="" width="600"/>

#### 转换支持大部分流上作业常用场景，大部分工作可以通过配置SQL实现流上处理逻辑
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_2.png" alt="" width="600"/>

#### Wormhole有Flow和Stream的概念，支持在一个物理Stream（对应一个Spark Stream）里通过并行处理多个逻辑Flow，使得User可以更加精细灵活的利用计算资源，User也可以对Stream进行精细化参数配置调整以更好平衡需求和资源
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_3_stream.png" alt="" width="600"/>

#### Wormhole也支持批处理Job，同样可以配置化实现处理逻辑并落到多个异构Sink，Flow和Job的配合可以很容易实现Lambda架构和Kappa架构
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_1.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_2.png" alt="" width="600"/>

#### User可以查看Project相关的Namespace/User/UDF/Resource
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_ns.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_user.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_6_udf.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_7_res.png" alt="" width="600"/>

#### User还可以监控Project正在运行的所有Flow/Stream的吞吐和延迟
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_8_mon.png" alt="" width="600"/>

#### 以上是简短的功能和用户体验预览，更多强大的细节功能请参见其他部分


## Latest Release

Please download the latest [RELEASE](https://github.com/edp963/wormhole/releases/download/0.3.0/wormhole-0.3.0.tar.gz).

## Get Help

- **Mailing list**: edp_support@groups.163.com
- **WeChat**: edpstack <img src="https://github.com/edp963/edp-resource/raw/master/WeChat.jpg" alt="" width="100"/>

## License

Wormhole is under the Apache 2.0 license. See the [LICENSE](https://github.com/edp963/wormhole/blob/master/LICENSE) file for details.

