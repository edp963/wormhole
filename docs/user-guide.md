---
layout: global
displayTitle: User Guide
title: User Guide
description: Wormhole WH_VERSION_SHORT User Guide page

---

* This will become a table of contents (this text will be scraped).
{:toc}
Wormhole 系统中有三类用户角色 Admin，User，App。本章介绍 User 类型用户的使用规范。

普通用户登陆后可以访问 Admin 授权的Project，具有管理 Stream，Flow，Job 的权限，可以使用Project 下所属的 Namespace，UDF 等。

## Stream 管理

#### 类型

理论上 Stream 可以处理所有类型的数据，为提升性能，针对 Hdfs 数据备份和分流功能作了相应优化，所以将 Stream 分为三种类型。

- default：可将数据写入 Kafka/RDBS/Elasticsearch/Hbase/Phoenix/Cassandra/MongoDB 系统中
- hdfslog：可将数据备份至 Hdfs 上，可以为 Job 提供数据源，实现 Kappa 架构
- routing：可将某 Topic 上数据分发到其他 Topic 中

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-stream-type.png" alt="" width="600"/>

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

- Stream 启动时会检查其所占用资源是否大于该 Project 下剩余可用资源，如果大于会提示资源不足，无法启动。可调小资源或停止其他流或调整该 Project 可用的计算资源
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

## Flow 管理

需指定 Stream，Source Namespace，Protocol，Sink Namespace，配置数据转换逻辑等。

#### Protocol

- increment 代表只处理 data_increment_data 协议的数据
- initial 代表只处理 data_initial_data 协议的数据
- all 代表处理两种协议的数据

#### Source Namespace

- 若 Wormhole 未对接 Dbus，源数据系统只支持 Kafka

- 若 Wormhole 已对接 Dbus，选择在 Dbus 中配置的源数据系统类型

- 可选的 Namespace 有一定的权限控制，其中 Source Namespace 是 Stream 对应 Kafka Instance 下的 Namespaces 与 Flow 所在 Project 下可访问 Namespaces 的交集；Sink Namespace 是 Flow 所在 Project 下可访问的Namespaces 且去除从 Dbus 系统同步的 Namespace

#### Sink Namespace

Sink Namespace 对应的物理表需要提前创建，表的 Schema 中是否需要创建 UMS 系统字段 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_active_（int 类型）`，根据以下策略判断须增加的字段：

- 源数据为 UMS 类型，则 Sink 表中需添加三个字段
- 源数据为 UMS_Extension 类型，若源数据 Schema 中配置了 `ums_ts_` 字段，Sink 表中须增加 `ums_ts_` 字段；若源数据 Schema 中配置了 `ums_ts_, ums_id_` 字段，Sink 表中须增加 `ums_ts_, ums_id_` 字段；若源数据 Schema 中配置了 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_op_（string 类型）` 字段，Sink 表中须增加 `ums_id_, ums_ts_, ums_active_` 字段。（注意：如果只配置了 `ums_ts_` 字段，向 Sink 表中写数据时只能选择 insert only 类型）

#### Result Fields

 配置最后输出的字段名称，All 代表输出全部字段，点击 Selected 可配置需要输出的字段名称，多个用逗号隔开

#### Sink Config

- Sink Config 项配置与所选系统类型相关，点击配置按钮后页面上方有对应系统的配置项例子
- 其中 "mutation_type" 的值有 "i" 和 "iud"，代表向 Sink 表中插数据时使用只增原则或增删改原则。如果为 "iud"，源数据中须有 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_op_（string 类型）` 字段，Sink 表中都须有 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_active_（int 类型）` 字段。若不配置此项，默认为 "iud"

#### Transformation

配置数据转换逻辑，支持 SQL 和自定义 Class 方式，可以配置多条转换逻辑，调整逻辑顺序

##### 自定义 Class

- pom中添加 wormhole/sparkxinterface 依赖

  ```
  <dependency>
     <groupId>edp.wormhole</groupId>
     <artifactId>wormhole-sparkxinterface</artifactId>
     <version>0.4.1</version>
  </dependency>
  ```

- clone wormhole github 项目，本地安装 wormhole-sparkxinterface jar 包

  ```
  方式1：wormhole 目录下执行安装全量包，大约8分钟左右

  mvn clean install package -Pwormhole 

  方式2：单独安装 wormhole-sparkxinterface 包，大约1分钟左右

  wormhole 目录下执行

  mvn clean install package

  wormhole/common/sparkxinterface 目录下执行

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

#### 修改 Flow

修改 Flow 时，不能修改所选 Stream，SourceNamespace 和 SinkNamespace，可以修改 Protocol 类型，Result Fields，Sink Config 和 Transformation 转换逻辑。

#### 启动 Flow

- Sink 端为 RDBMS 时，需将相应的 jdbc connector jar 放至 $SPARK_HOME/jars 目录下，然后启动或重启 Stream

- 点击启动按钮，后台会将 Flow 的信息提交给 Stream，且会将 Flow Source Namespace 和 Stream Join Namespaces 所在 Topic 绑定到 Stream 上。
- Stream 接收到 Flow 指令后解析，若成功解析，返回成功信息，Flow 的状态由 starting 转成 running；若解析失败，Flow 的状态由 starting 转成 failed。可在 Yarn 上查看 Driver 日志，根据错误提示重新配置 Flow

#### 生效 Flow

- Flow 运行过程中，可修改 Flow 逻辑，点击生效按钮，动态更新 Stream 的配置
- 若修改了 Flow 所用 Namespaces/Databases/Instances 的配置，须点击生效按钮，动态更新 Stream 的配置
- 点击生效按钮后，Flow 的状态转换为 updating，收到 Stream 反馈后转换成 running 或 failed

#### 停止 Flow

停止 Flow 时向 Stream 发送取消指令，并检查 Stream 其他 Flow 对应的 Topic 是否包含该 Flow 对应的 Topic，如果不包含则取消 Stream 与该 Topic 的绑定关系。

#### Flow 状态转换

- new 代表新建后还未启动
- starting 代表正在启动
- running 代表正在运行
- suspending 代表挂起状态。Stream 处于非 running 状态时，Flow 的状态由 starting/running/updating 转换为 suspending 状态。Stream 处于 running 状态后，Flow 状态会自动转换为 running/failed 状态
- failed 代表启动失败
- stopping 代表正在停止
- stopped 代表已经停止

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-flow-list.png" alt="" width="600"/>

### Job

借助 Job 可轻松实现 Lambda 架构和 Kappa 架构。

首先使用 hdfslog Stream 将源数据备份到 Hdfs，Flow 出错或需要重算时，可配置 Job 重算。具体配置可参考Stream 和 Flow。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-job-source.png" alt="" width="600"/>

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-job-sink.png" alt="" width="600"/>

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user-guide-job-list.png" alt="" width="600"/>

