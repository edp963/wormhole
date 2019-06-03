---
layout: global
displayTitle: Quick Start
title: Quick Start
description: Wormhole Quick Start page
---

In this chapter, the application process of Wormhole is introduced through the implementation example of a streaming project.

------This chapter introduces how to use Wormhole application through the implementation example of a streaming project.

Business requirement: process data in Kafka in real time, associate a certain table in Mysql database during the processing of data, transform and filter data and write into Hbase system.

### Admin

**1. Log in the system and create a normal user.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-createUser.png" alt="" width="600"/>

**2. Create Source Namespace.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-instance-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-database-create.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-source-namespace-create.png" alt="" width="600"/>

**3. Create source topic in Kafka cluster to generate test data. If Source Namespace is `kafka.test.source.ums_extension.*.*.*`,  key of Kafka message should be set as `data_increment_data.kafka.test.source.ums_extension.*.*.*`. Data sample of Kafka is as follows:**

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

**4. Configure Source Namespace Schema.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick_start-source-schema.png" alt="" width="600"/>

**5. Create Sink Namespace.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-create-sink-ns.png" alt="" width="600"/>

**6. Create sink topic in Kafka cluster.**

**7. Create Lookup Namespace.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-create-lookup-ns.png" alt="" width="600"/>

**8. Create Project and authorize certain Users with certain Namespaces.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-project.png" alt="" width="600"/>

### User

**1. Log in the system; create and start Stream.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-stream_configs.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-stream_running.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/qiuck-start-stream_start.png" alt="" width="600"/>

**2. Create and start Flow.**

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-source.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-sink.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-transform.png" alt="" width="600"/>

   <img src="https://github.com/edp963/wormhole/raw/master/docs/img/quick-start-flow-start.png" alt="" width="600"/>


**You can refer to other chapters for detailed steps and configuration instructions. **