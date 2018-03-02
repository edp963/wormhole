---
layout: global
displayTitle: Quick Start
title: Quick Start
description: Wormhole WH_VERSION_SHORT Quick Start page
---

本章节以一个流式项目的实施示例介绍 Wormhole 页面的使用流程。

业务需求：实时处理 Kafka 中数据，处理过程中关联 Mysql 数据库某表，然后转换过滤数据，写入 Hbase 系统中。

#### Admin 用户

**1. Admin 用户登录系统后创建普通用户**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-createUser.png" alt="" width="600"/>


**2. Admin 创建 Source Namespace**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-instance-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-database-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-namespace-create.png" alt="" width="600"/>


**3. Kafka 集群中创建 source topic，并生成测试数据，对应 Source Namespace，Kafka 消息的 key 应设置为 `data_increment_data.kafka.test.source.ums_extension.*.*.*`。Kafka 数据样例如下：**

   ```
   {
       "id": 1,
       "name": "test",
       "phone": [
           "18074546423",
           "13254356624"
       ],
       "message": "2017-06-27 14:14:04,557|INFO",
       "address": {
           "province": "Beijing",
           "city": "Beijing"
       },
       "contacts": [
           {
               "name": "test",
               "phone": [
                   "18074546452",
                   "13254356643"
               ]
           }
       ],
       "time": "2017-12-22 10:00:00"
   }
   ```

**4. Admin 配置 Source Namespace Schema**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick_start-source-schema.png" alt="" width="600"/>


**5. Admin 创建 Sink Namespace**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-create-sink-ns.png" alt="" width="600"/>


**6. Kafka 集群中创建 sink topic**


**7. Admin 创建 Lookup Namespace**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-lookup-ns.png" alt="" width="600"/>


**8. Admin 创建 Project 并授权 Namespaces 和 Users**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-project.png" alt="" width="600"/>

#### User 用户

**1. User 登录系统后创建并启动 Stream**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-stream_configs.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-stream_running.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-stream_start.png" alt="" width="600"/>


**2. User 创建并启动 Flow**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-source.png" alt="" width="600"/>
   
   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sink.png" alt="" width="600"/>
   
   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-transform.png" alt="" width="600"/>
   
   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start.png" alt="" width="600"/>


**具体步骤及配置说明请参考其他章节~~**