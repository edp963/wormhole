---
layout: global
displayTitle: User Guide
title: User Guide
description: Wormhole User Guide page

---

* This will become a table of contents (this text will be scraped).
{:toc}
Wormhole 系统中有三类用户角色 Admin，User，App。本章介绍 User 类型用户的使用规范。

普通用户登陆后可以访问 Admin 授权的Project，具有管理 Stream，Flow，Job 的权限，可以使用Project 下所属的 Namespace，UDF 等。

## Stream 管理

### Spark Stream管理

#### 类型

理论上Stream 可以处理所有类型的数据，为提升性能，针对 Hdfs 数据备份和分流功能作了相应优化，所以将 Stream 分为三种类型。

- default：可将数据写入 Kafka/RDBS/Elasticsearch/Hbase/Phoenix/Cassandra/MongoDB 系统中
- hdfslog：可将数据备份至 Hdfs 上，可以为 Job 提供数据源，实现 Kappa 架构
- routing：可将某 Topic 上数据分发到其他 Topic 中

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-type-spark.png" alt="" width="600"/>

#### 资源配置

-  Driver，Executor 内存和 Vcore 资源大小设置
-  定时调度的时间间隔设置
-  获取 Kafka RDD 后重新分区的分区个数设置，建议设置为 Executor 的个数
-  每批次可从 Kafka Topic Partition 中获取的最大数据量设置，单位为 M

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-configs.png" alt="" width="600"/>

#### Topic 绑定

Stream 消费哪些 Topic 根据 Flow 的启停自动绑定和注销。

Flow 启动时检查其 Source Namespace 对应 Topic 是否已绑定在 Stream 上，若已绑定不会重复注册，根据 Stream 目前已消费到的 Offset 继续执行。否则将该 Topic 绑定到 Stream 上，初始 Offset 设置为该 Topic 的最新 Offset。

Flow 停止时检查其 Source Namespace 对应 Topic 是否对应该 Stream 上其他 Flow，若无则注销该 Stream 与 Topic 的绑定关系。

#### UDF 绑定

启动 Stream 时可以选择需要加载的 UDF，也可以取消已选择的 UDF。Stream 启动后可以点击生效按钮，选择需要增加的 UDF，Stream 不需要重启，可动态加载新 UDF。

#### 启动

- 选择 UDF
- 配置每个 Topic 的消费速度 Rate (单位为条/秒)
- 配置每个 Topic Partition 消费的起始 Offset

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-start.png" alt="" width="600"/>

**注意事项：**

- Stream 启动时会检查其所占用资源是否大于该 Project 下剩余可用资源，如果大于会提示资源不足，无法启动。可调小资源或停止其他流或调整该 Project 可用的计算资源。
- 若正常启动后，一直处于 starting 状态或转为 failed 状态，可点击查看日志按钮，根据日志错误信息调整，重启即可。

#### 生效

Stream 运行过程中支持 UDF 热部署，支持动态调整 Topic 消费的 Offset 和 Rate。

**注意事项：**

若需要调整 Stream configs 项的配置，需要重启 Stream。

#### 状态转换

- new 代表新建后还未启动
- starting 代表正在启动
- waiting 代表已提交到 Yarn 队列
- running 代表正在运行
- failed 代表启动失败或运行过程中失败
- stopping 代表正在停止
- stopped 代表已经停止

Stream 状态转换图如下，其中 refresh 代表 Refresh 按钮，start 代表启动按钮，stop 代表停止按钮。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-state-exchange.png" alt="" width="600"/>

### Flink Stream 管理

#### 类型

Flink中支持的Stream类型只有default，理论上可以处理所有类型的数据，将数据写入Kafka/RDBS/Elasticsearch/Hbase/Phoenix/Cassandra/MongoDB系统中，但目前只支持处理UMS数据类型，目标系统只支持Kafka，UMS_Extension类型及其他目标系统会在后续版本支持

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-type-flink.png" alt="" width="600"/>

#### 资源配置

- JobManager内存
- TaskManager数量
- 每个TaskManager内存及slots数量

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-configs-flink.png" alt="" width="600"/>

#### 启动

- 直接点击启动按钮即可启动，后台会在Yarn上提交启动Flink Job Manager命令

  <img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-start-flink.png" alt="" width="600"/>

## Flow 管理

需指定 Stream，Source Namespace，Protocol，Sink Namespace，配置数据转换逻辑等。

### 选择 Stream

#### Spark Stream

- 支持Default、Hdfslog和Routing三种类型

#### Flink Stream

- 设置Parallelism并行度：Flow在Flink Stream中处理的并行度

### Protocol

- increment 代表只处理 data_increment_data 协议的数据
- initial 代表只处理 data_initial_data 协议的数据
- backfill 代表只处理 data_batch_data 协议的数据（通过Wormhole Job回灌到Kafka中的数据协议类型为 data_batch_data）

### Source Namespace

- 若 Wormhole 未对接 DBus，源数据系统只支持 Kafka
- 若 Wormhole 已对接 DBus，选择在 DBus 中配置的源数据系统类型
- 可选的 Namespace 有一定的权限控制，其中 Source Namespace 是 Stream 对应 Kafka Instance 下的 Namespaces 与 Flow 所在 Project 下可访问 Namespaces 的交集；Sink Namespace 是 Flow 所在 Project 下可访问的Namespaces 且去除从 DBus 系统同步的 Namespace
- 注：Flink Flow暂时只支持UMS类型数据源，用户自定义json类型数据源将在后续版本进行支持

### Sink Namespace

Sink Namespace 对应的物理表需要提前创建，表的 Schema 中是否需要创建 UMS 系统字段 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_active_（int 类型）`，根据以下策略判断须增加的字段：

- 源数据为 UMS 类型，则 Sink 表中需添加三个字段
- 源数据为 UMS_Extension 类型，若源数据 Schema 中配置了 `ums_ts_` 字段，Sink 表中须增加 `ums_ts_` 字段；若源数据 Schema 中配置了 `ums_ts_, ums_id_` 字段，Sink 表中须增加 `ums_ts_, ums_id_` 字段；若源数据 Schema 中配置了 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_op_（string 类型）` 字段，Sink 表中须增加 `ums_id_, ums_ts_, ums_active_` 字段。（注意：如果只配置了 `ums_ts_` 字段，向 Sink 表中写数据时只能选择 insert only 类型）
- 注：Flink Flow暂时只支持写入kafka系统，其他系统将在后续版本进行支持

### Result Fields

 配置最后输出的字段名称，All 代表输出全部字段，点击 Selected 可配置需要输出的字段名称，多个用逗号隔开

### Sink Config

- Sink Config 项配置与所选系统类型相关，点击配置按钮后页面上方有对应系统的配置项例子
- 其中 "mutation_type" 的值有 "i" 和 "iud"，代表向 Sink 表中插数据时使用只增原则或增删改原则。如果为 "iud"，源数据中须有 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_op_（string 类型）` 字段，Sink 表中都须有 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_active_（int 类型）` 字段。若不配置此项，默认为 "iud"

### Transformation

#### Spark Flow Transformation

配置数据转换逻辑，支持 SQL 和自定义 Class 方式，可以配置多条转换逻辑，调整逻辑顺序

##### 自定义 Class

- pom中添加 wormhole/sparkxinterface 依赖

  ```
  <dependency>
     <groupId>edp.wormhole</groupId>
     <artifactId>wormhole-sparkxinterface</artifactId>
     <version>0.5.2-beta</version>
  </dependency>
  ```

- clone wormhole github 项目，本地安装 wormhole-sparkxinterface jar 包

  ```

  安装wormhole-sparkxinterface包至本地maven仓库

  wormhole/common/util目录下执行

  mvn clean install package

  wormhole/ums目录下执行

  mvn clean install package

  wormhole/common/sparkxinterface目录下执行

  mvn clean install package
  ```

- 继承 并实现 wormhole/common/sparkxinterface module 下的 edp.wormhole.sparkxinterface.swifts.SwiftsInterface 接口，可参考 wormhole/sparkx module 下的 edp.wormhole.swifts.custom.CustomTemplate 类

- 编译打包，将带有 Dependencies 的 Jar 包放置在 $SPARK_HOME/jars 目录下

- 页面配置时，选择 Custom Class，输入方法名全路径，如 edp.wormhole.swifts.custom.CustomTemplate

- Flow 启动或生效，重启 Stream

##### SQL

###### Lookup SQL

Lookup SQL 可以关联流下其他系统数据，如 RDBS/Hbase/Redis/Elasticsearch 等，规则如下。

若 Source Namespace 为 kafka.edp_kafka.udftest.udftable，Lookup Table 为 RDBMS 系统，如 mysql.er_mysql.eurus_test 数据库下的 eurususer 表，Left Join 关联字段是 id，name，且从 Lookup 表中选择的字段 id，name 与主流上kafka.edp_kafka.udftest.udftable 中的字段重名，SQL语句如下：

```
select id as id1, name as name1, address, age from eurus_user where (id, name) in (kafka.edp_kafka.udftest.udftable.id, kafka.edp_kafka.udftest.udftable.name);
```

若 Source Namespace为 kafka.edp_kafka.udftest.udftable，Union Table为 mysql.ermysql.eurustest 数据库下的 eurus_user 表，eurus_user 表中须含有与源数据相同的 UMS 系统字段，SQL 语句规则同上。

**注意事项：**

- Lookup SQL 中只能有一条 SQL 含有 group by，且 group by 字段要包含在 where in 条件中
- where 条件中的字段必须选出
- select 出的字段中若有与Flow Source Namespace 重名字段，select 时需须重命名
- union 操作会以主流当前数据的情况做基准，查找出的数据如果不包含主流的某些字段，会自动填 null 值;如果比主流多出某些字段，会自动去掉。
- 每一个配置框中只能填写一条SQL语句

###### Spark SQL

Spark SQL 用于处理 Source Namespace 数据，from 后面直接接表名即可。Spark SQL 支持使用 UDF 方法，UDF 方法须包含在该 Flow 对应的 Stream 中。

###### Stream Join SQL

- 选择要关联的其他 Source Namespace，可关联多个 Source Namespace
- Stream Join SQL 处理过程中会将没有关联上的数据保存到 HDFS 上，data retention time 代表数据的有效期
- select 语句规则同 Spark SQL

#### Flink Flow Transformation

配置数据转换逻辑，支持 SQL ，可以配置多条转换逻辑，调整逻辑顺序。

支持两种事件模型Processing Time和Event Time。Processing Time为数据进入到Flink的时间，即数据进入source operator时获取时间戳；Event Time为事件产生的时间，即数据产生时自带时间戳，在Wormhole系统中对应```ums_ts_```字段。暂时不支持Event Time，只支持Processing Time。

##### CEP

Wormhole Flink版对传输的流数据除了提供Lookup SQL、Flink SQL两种Transformation操作之外，还提供了CEP（复杂事件处理）这种转换机制。

 CEP里面的几个必填属性的含义和用途如下：

须设定以下参数：

1）WindowTime：它是指在触发了符合Begin Pattern的数据记录后的窗口时间，如果watermark的time超过了触发时间+窗口时间，本次pattern结束

2）KeyBy：依据数据中的哪个字段来做分区，举个例子，比如，现在有一条数据，它的schema包括ums_id_,ums_op_,ums_ts_,value1,value2这几个字段，这里选定value1来做分区的依赖字段，那么，value1字段相同的数据将被分配到同一个分组上。CEP操作将分别针对每一分组的数据进行处理

3）Strategy：策略分为NO_SKIP和SKIP_PAST_LAST_EVENT两种，前者对应数据滑动策略，后者对应数据滚动策略，具体区别可以借鉴下面的例子：（假设一次处理4条）

- 数据滑动

  a1 a2 a3 a4 ........

  a2 a3 a4 a5 ........

  a3 a4 a5 a6 ........

- 数据滚动

  a1 a2 a3 a4 ........

  a5 a6 a7 a8  ........

  a9 a10 a11 a12 .....

4）Output：输出结果的形式，大致分为三类：Agg、Detail、FilteredRow

- Agg：将匹配的多条数据做聚合，生成一条数据输出,例：field1:avg,field2:max（目前支持max/min/avg/sum）
- Detail：将匹配的多条数据逐一输出
- FilteredRow：按条件选择指定的一条数据输出，例：head/last/ field1:min/max

5）Pattern：筛选数据的规则。每个CEP由若干个pattern组成。

每个Pattern包括以下三个必填信息：

- Operator:操作类型，每个CEP中的第一个Pattern，其Operator只能为begin，其后的每个Pattern Operator只能为next、followed by、not next、not followed by这四种类型中的一种，其含义分别为：
  - Begin：每个模式必须以一个初始状态开始，Begin用来构建初始Pattern
  - Next：会追加一个新的Pattern对象到既有的Pattern之后，它表示当前模式运算符所匹配的事件必须是严格紧邻的，这意味着两个被匹配的事件必须是前后紧邻，中间没有其他元素
  - FollowedBy：会追加一个新的Pattern到既有的Pattern之后（其实返回的是一个FollowedByPattern对象，它是Pattern的派生类），它表示当前运算符所匹配的事件不必严格紧邻，这意味着其他事件被允许穿插在匹配的两个事件之间
  - NotNext：增加一个新的否定模式。匹配(否定)事件必须直接输出先前的匹配事件(严格紧邻)，以便将部分匹配丢弃
  - NotFollowedBy：会丢弃或者跳过已匹配的事件；(注：not FollowedBy不能为最后一个Pattern)
- Quantifier：用来匹配满足该pattern的一条或多条数据。目前配置包括：一条及以上，指定条数，指定条数及以上；这块需要特殊说明的是，notNext、not FollowedBy这两种操作类型无法设置Quantifier。它代表的意思就是后面没有符合该Pattern的数据才属于符合条件
- Conditions：具体的判断依据，在这一项中，用户可以具体针对数据的某一个或多个属性进行判断条件设置，例如，我可以设置只有符合value1 like a and value2 >=10的数据才是符合条件的数据

##### SQL

####### Lookup SQL

具体可参考Spark Flow Transformation的Lookup SQL章节

####### Flink SQL

Flink SQL 用于处理 Source Namespace 数据，from 后面直接接表名即可。Flink SQL UDF 功能会在后续版本支持。

### 修改 Flow

修改 Flow 时，不能修改所选 Stream，SourceNamespace 和 SinkNamespace，可以修改 Protocol 类型，Result Fields，Sink Config 和 Transformation 转换逻辑

### 启动 Flow

#### 启动 Spark Flow

- Sink 端为 RDBMS 时，需将相应的 jdbc connector jar 放至 $SPARK_HOME/jars 目录下，然后启动或重启 Stream
- 点击启动按钮，后台会将 Flow 的信息提交给 Stream，且会将 Flow Source Namespace 和 Stream Join Namespaces 所在 Topic 绑定到 Stream 上
- Stream 接收到 Flow 指令后解析，若成功解析，返回成功信息，Flow 的状态由 starting 转成 running；若解析失败，Flow 的状态由 starting 转成 failed。可在 Yarn 上查看 Driver 日志，根据错误提示重新配置 Flow

#### 启动 Flink Flow

- Stream running状态下才可以启动Flow
- 配置每个Topic Partition消费的起始Offset，可配置用户自定义Topic
- 点击启动按钮，后台会向对应Flink Stream JobManager上提交创建TaskManager请求
- 启动Flow后可点击查看日志按钮，若创建成功状态会转至running状态，若创建失败状态会转至failed状态，可根据日志错误提示重新配置

### 生效 Flow

#### 生效 Spark Flow

- Flow 运行过程中，可修改 Flow 逻辑，点击生效按钮，动态更新 Stream 的配置
- 若修改了 Flow 所用 Namespaces/Databases/Instances 的配置，须点击生效按钮，动态更新 Stream 的配置
- 点击生效按钮后，Flow 的状态转换为 updating，收到 Stream 反馈后转换成 running 或 failed

### 停止 Flow

#### 停止 Spark Flow

停止 Flow 时向 Stream 发送取消指令，并检查 Stream 其他 Flow 对应的 Topic 是否包含该 Flow 对应的 Topic，如果不包含则取消 Stream 与该 Topic 的绑定关系

#### 停止 Flink Flow

点击停止按钮提交取消对应Flink Task请求

### Flow 状态转换

- new 代表新建后还未启动
- starting 代表正在启动
- running 代表正在运行
- suspending 代表挂起状态。Stream 处于非 running 状态时，Flow 的状态由 starting/running/updating 转换为 suspending 状态。Stream 处于 running 状态后，Flow 状态会自动转换为 running/failed 状态
- failed 代表启动失败
- stopping 代表正在停止
- stopped 代表已经停止

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-flow-list.png" alt="" width="600"/>

### Flow 漂移

#### Flow漂移规则

- 只有spark default flow可以迁移，其他flow不能迁移
- 只能迁移至与原flow对应stream消费同一kafka集群的stream，即对应kafka instance url相同
- spark default flow漂移规则见下表

| Flow状态                     | 新Stream状态                   | 新Flow状态     |
| -------------------------- | --------------------------- | ----------- |
| new/stopped/failed         | _                           | new/stopped |
| starting/updating/stopping | 不可迁移                        | 不可迁移        |
| suspending                 | _                           | stopped     |
| running                    | new/stopping/stopped/failed | stopped     |
| running                    | starting/waiting/running    | starting    |

#### running flow topic offset 确定规则：

- 若新stream未注册该topic，注册该topic，offset取老stream中反馈的最新offset (feedback_stream_offset表中对应stream/topic ums_ts最大行对应offset)
- 若新stream已注册该topic，取两stream中小offset
  - stream处于starting/waiting状态，取rel_stream_intopic表中stream/topic对应offset
  - stream处于running状态，若feedback_stream_offset表中对应stream/topic最大ums_ts大于stream启动时间，取若feedback_stream_offset表中offset，否则取rel_stream_intopic表中offset
  - 取两stream较小offset

## Job

借助 Job 可轻松实现 Lambda 架构和 Kappa 架构。

首先使用 hdfslog Stream 将源数据备份到 Hdfs，Flow 出错或需要重算时，可配置 Job 重算。具体配置可参考Stream 和 Flow。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-job-source.png" alt="" width="600"/>

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-job-sink.png" alt="" width="600"/>

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-job-list.png" alt="" width="600"/>

## 监控预警

Stream运行过程中会将每批处理的错误信息，offset信息，数据量信息和延时等信息发送至wormhole_feedback topic中。Wormhole Web应用负责消费这些信息，其中错误信息和offset信息保存在MySQL数据库中，数据量信息和延时统计信息保存在Elasticsearch中。

Wormhole项目内Performance页面通过嵌入Grafana Dashboard展示每个项目下Stream/Flow吞吐和延时信息。（使用此功能Wormhole配置文件中须配置Grafana/Elasticsearch信息） 

吞吐和延时信息从Stream/Flow两个维度展示，监控项说明如下。

#### Latency

- ReceivedDelay   每批次开始处理时间 — 每批次随机取一条数据 ums_ts_
- PreprocessDelay    每批次预处理完成时间 — 每批次开始处理时间
- SwiftsDelay  每批次Transformation逻辑处理完成时间 — 每批次预处理完成时间
- WriteSinkDely  每批次Sink目标表完成时间 — 每批次Transformation逻辑处理完成时间
- WormholeDelay  每批次Sink目标表完成时间 — 每批次开始处理时间

#### Records

每批次处理的数据条数，对于UMS类型数据，指每批处理的ums消息payload中tuple总条数。

#### Throughput

Records/WormholeDelay

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-monitor.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-flow-monitor.png" alt="" width="600"/>
