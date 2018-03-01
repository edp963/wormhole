---
layout: global
displayTitle: Quick Start
title: Quick Start
description: Wormhole WH_VERSION_SHORT Quick Start page
---

本章节以一个流式项目的实施示例介绍 Wormhole 页面的使用流程。

业务需求：实时处理Kafka 中数据，处理过程中关联 Mysql 数据库某表，然后转换过滤数据，写入Hbase系统中。

#### Admin

1. Admin 用户登录系统后创建普通用户

![quick-start-createUser](/Users/swallow/IdeaProjects/wormhole/docs/img/quick-start-createUser.png)

2. Admin 创建 Source Namespace

![quick-start-source-instance-create](/Users/swallow/IdeaProjects/wormhole/docs/img/quick-start-source-instance-create.png)

![quick-start-source-database-create](/Users/swallow/IdeaProjects/wormhole/docs/img/quick-start-source-database-create.png)

![quick-start-source-namespace-create](/Users/swallow/IdeaProjects/wormhole/docs/img/quick-start-source-namespace-create.png)

3. Kafka 集群中创建 source topic，并生成测试数据，对应 Source Namespace，Kafka 消息的 key 应设置为`data_increment_data.kafka.test.source.ums_extension.*.*.*`。Kafka 数据样例如下：

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

4. Admin 配置 Source Namespace Schema

![quick_start-source-schema](/Users/swallow/IdeaProjects/wormhole/docs/img/quick_start-source-schema.png)

5. Admin 创建 Sink Namespace

![qiuck-start-create-sink-ns](/Users/swallow/IdeaProjects/wormhole/docs/img/qiuck-start-create-sink-ns.png)

6. Kafka 集群中创建 sink topic
7. Admin 创建 Lookup Namespace

![quick-start-create-lookup-ns](/Users/swallow/IdeaProjects/wormhole/docs/img/quick-start-create-lookup-ns.png)

8. Admin 创建 Project 并授权 Namespaces 和 Users

![quick-start-project](/Users/swallow/IdeaProjects/wormhole/docs/img/quick-start-project.png)

9. User 登录系统后创建并启动 Stream
10. User 创建并启动 Flow

具体步骤及配置说明请参考其他章节~~