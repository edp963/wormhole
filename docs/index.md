---
layout: global
displayTitle: Wormhole Overview
title: Overview
description: Wormhole documentation homepage
---
> 来自[宜信](http://www.creditease.com/)[技术研发中心](http://crdc.creditease.cn/)的流式处理平台

**Wormhole 是一个一站式流式处理云平台解决方案（SPaaS - Stream Processing as a Service）。**

Wormhole 面向大数据流式处理项目的开发管理运维人员，致力于提供统一抽象的概念体系，直观可视化的操作界面，简单流畅的配置管理流程，基于 SQL 即可完成的业务逻辑开发方式，并且屏蔽了流式处理的底层技术细节，极大的降低了数据项目管理运维门槛，使得大数据流式处理项目的开发管理运维变得更加轻量敏捷可控可靠。

## Documentation

Please refer to [Wormhole用户手册](https://edp963.github.io/wormhole).

## Architecture

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/wh4_pipeline_overview.png" alt="" width="600"/>

### 设计理念

- **统一 DAG 高阶分形抽象**
  - 构建由 Source DataSys，Kafka Topic，Spark Stream（Flink Stream），Sink DataSys 组成的物理 DAG
  - 每个物理 DAG 里可以并行处理多个由 Source Namespace，Flow，Sink Namespace 组成的逻辑 DAG
  - 每个 Flow 本身是典型的 Spark RDD DAG
- **统一通用流消息 UMS 协议抽象**
  - UMS 是 Wormhole 定义的流消息协议规范
  - UMS 试图抽象统一所有结构化消息
  - UMS 自身携带结构化数据 Schema 信息
  - Wh4 支持用户自定义半结构化 JSON 格式
- **统一数据逻辑表命名空间 Namespace 抽象**
  - Namespace 唯一定位所有数据存储所有结构化逻辑表
  - [Data System].[Instance].[Database].[Table].[Table Version].[Database Partition].[Table Partition]

### 主要特性

- **支持可视化，配置化，SQL 化开发实施流式项目**
- **支持指令式动态流式处理的管理，运维，诊断和监控**
- **支持统一结构化 UMS 消息和自定义半结构化 JSON 消息**
- **支持处理增删改三态事件消息流**
- **支持单个物理流同时并行处理多个逻辑业务流**
- **支持流上 Lookup Anywhere，Pushdown Anywhere**
- **支持基于业务策略的事件时间戳流式处理**
- **支持 UDF 的注册管理和动态加载**
- **支持多目标数据系统的并发幂等入库**
- **支持多级基于增量消息的数据质量管理**
- **支持基于增量消息的流式处理和批量处理**
- **支持 Lambda 架构和 Kappa 架构**
- **支持与三方系统无缝集成，可作为三方系统的流控引擎**
- **支持私有云部署，安全权限管控和多租户资源管理**

## Experience

#### Admin 可以创建 Project/Namespace/User/UDF，并且可查看所有 Flow/Stream/Job
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_1.png" alt="" width="600"/>

#### Admin 可以为 Project 分配 Namespace 资源/User 资源/UDF 资源/计算资源，以支持多租户资源隔离
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_2.png" alt="" width="600"/>

#### User 可以对自己有权限的 Project 进行开发实施和管理运维工作
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_1_project.png" alt="" width="600"/>

#### User 可以通过简单配置步骤即可搭建起一个流式作业 pipeline（Flow），只需关注数据从哪来到哪去和如何转换处理
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_1.png" alt="" width="600"/>

#### 转换支持大部分流上作业常用场景，大部分工作可以通过配置 SQL 实现流上处理逻辑
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_2.png" alt="" width="600"/>

#### Wormhole 有 Flow 和 Stream 的概念，支持在一个物理 Stream（对应一个 Spark Stream）里通过并行处理多个逻辑 Flow，使得 User 可以更加精细灵活的利用计算资源，User 也可以对 Stream 进行精细化参数配置调整以更好平衡需求和资源
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_3_stream.png" alt="" width="600"/>

#### Wormhole 也支持批处理 Job，同样可以配置化实现处理逻辑并落到多个异构 Sink，Flow 和 Job 的配合可以很容易实现 Lambda 架构和 Kappa 架构
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_1.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_2.png" alt="" width="600"/>

#### User 可以查看 Project 相关的 Namespace/User/UDF/Resource
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_ns.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_user.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_6_udf.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_7_res.png" alt="" width="600"/>

#### User 还可以监控 Project 正在运行的所有 Flow/Stream 的吞吐和延迟
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_8_mon.png" alt="" width="600"/>

#### 以上是简短的功能和用户体验预览，更多强大的细节功能请参见其他部分

## Latest Release

Please download the latest RELEASE(链接：https://pan.baidu.com/s/1CYu39S-3TcWTJsRDXqFuHw  提取码：oo2o).

## Get Help

- **Mailing list**: edp_support@groups.163.com
- **WeChat**: edpstack <img src="https://github.com/edp963/edp-resource/raw/master/WeChat.jpg" alt="" width="100"/>

## License

Wormhole is under the Apache 2.0 license. See the [LICENSE](https://github.com/edp963/wormhole/blob/master/LICENSE) file for details.
