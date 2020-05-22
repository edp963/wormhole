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

#### 消费kafka中无key数据

如果绑定的topic中数据没有key，则可设定是否启用默认的kafka key，在specail config中设置{"useDefaultKey":true}，会将注册到该stream的第一个flow的source namespace作为这个topic中数据的key，该stream中同source namespace的flow就可以消费这个topic。如果绑定的topic中数据有key，则按照数据的key进行处理（0.6.3及之后版本支持）

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

Flink中支持的Stream类型只有default，支持异构sink，包括Kafka/RDBS/Elasticsearch/Hbase/Phoenix/Cassandra/MongoDB系统中，数据类型支持处理UMS数据类型和用户自定义UMS_Extension类型

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

### Sink Namespace

Sink Namespace 对应的物理表需要提前创建，表的 Schema 中是否需要创建 UMS 系统字段 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_active_（int 类型）`，根据以下策略判断须增加的字段：

- 源数据为 UMS 类型，为实现幂等更新和最终一致性，流处理的最后结果会保留上述三个字段，Sink 表中必须添加上述三个字段
- 源数据为 UMS_Extension 类型，以用户配置的SQL及Result Fields为准

### Table Keys

设置sink表的table keys，用于幂等的实现。如果有多个，用逗号隔开。

### Result Fields

 配置最后输出的字段名称，All 代表输出全部字段，点击 Selected 可配置需要输出的字段名称，多个用逗号隔开

### Sink Config

Sink Config 项配置与所选系统类型相关，点击配置按钮后页面上方有对应系统的配置项例子

#### 配置数据插入方式（只增加or增删改）

其中 "mutation_type" 的值有 "i" 和 "iud"，代表向 Sink 表中插数据时使用只增原则或增删改原则。如果为 "iud"，源数据中须有 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_op_（string 类型）` 字段，Sink 表中都须有 `ums_id_（long 类型）, ums_ts_（datetime 类型）, ums_active_（int 类型）` 字段。若不配置此项，默认为 "iud"

**注意事项：**

- 源数据为 UMS_Extension 类型时，若"mutation_type"为"iud"，源schema中必须配置与ums三个系统字段的映射，并且SQL中须显示选出这三个系统字段


#### 分表幂等

针对关系型数据库，为了减小ums_id、ums_op与ums_ts字段对业务系统的侵入性，可单独将这三个字段和table keys单独建立一个表，原业务表保持不变。假设ums_id、ums_op、ums_ts和table key组成的表名为umsdb，那么分表幂等的配置为：

`{"mutation_type":"split_table_idu","db.function_table":"umsdb"}`

#### ums_uid_字段输出

默认配置中ums_uid_字段会被过滤掉，不会写入sink端，通过配置sink_uid可将ums_uid_字段写入目标库

```
{"sink_uid":true}
```

#### sink分批读/写

Sink时支持分批读和分批写，批次大小配置项为batch_size

`{"batch_size":"10000"}`

#### sink kudu表名带特殊字符处理

impala建的kudu表中表名可能带"."等特殊字符，如果在namespace中将"."加入，就会影响wormhole对namespace分割处理，可以sink config中配置连接符解决（0.6.3及之后版本支持）。例如kudu的表名为impala::dbname.tablename，namespace中database可配置为impala::dbname，table可配置为tablename，sinkconfig中配置：{"table_connect_character":"."}即可

#### sink hbase设置版本字段进行幂等

Sink hbase可以设置列版本号字段，进行幂等：{"hbase.version.column":"ums_id_"}，如果不配置，则按照wormhole原来的方式进行幂等（0.6.3及之后版本支持）

#### sink es相关配置
index时间后缀配置，配置项为index_extend_config，例如{"index_extend_config":"_yyyy-MM-dd"}
访问header配置，配置项header_config，例如{"header_config":{"content-type":"application/json"}}（0.6.3之后版本支持）

Sink hbase可以设置列版本号字段，进行幂等：{"hbase.version.column":"ums_id_"}，如果不配置，则按照wormhole原来的方式进行幂等（0.6.3及之后版本支持）

#### 配置安全认证的sink kafka

在用户需要向启用了kerberos安全认证的kafka集群Sink数据时，需要在sink config里面做如下配置：{"kerberos":true}，默认情况下，是向未启用kerberos认证的kafka集群Sink数据（0.6.1及之后版本）

#### sink clickhouse

wormhole sink clickhouse支持分布式和本地写两种，如果instance是distributed节点，可以值sink config中配置{"ch.engine":"distributed"}。如果是merge tree节点连接地址用逗号分隔即可，sink config中配置{"ch.engine":"mergetree"}。按照merger tree 方式写入，现在wh支持的分发方式是xxHash64

#### 用户自定义sink

Wormhole 0.6.1及之后版本支持用户自定义sink

1、编写自定义sink class

（1）在wormhole项目中建立customer sink class流程

- clone wormhole github 项目

- 在wormhole/sinks/……/edp/wormhole/sinks/目录下建相应的customer sink class，该class需要继承edp.wormhole.publicinterface.sinks.SinkProcessor，并实现process方法

- 打包

- - 到wormhole/sinks目录下执行mvn clean install
  - 如果使用sparkx，到wormhole/sparkx目录下执行mvn clean install；如果使用的是flinkx，则到wormhole/flinkx下执行该命令）

- 替换线上包

- - 如果使用的是sparkx，将生成的wormhole/sparkx/target目录下的wormhole-ums_1.3-sparkx_2.2-0.6.3-jar-with-dependencies替换到线上wormhole app/目录下的该文件
  - 如果使用的是flinkx，则将wormhole/flinkx/target目录下wormhole-ums_1.3-flinkx_1.5.1-0.6.3-jar-with-dependencies替换线上文件

（2）在用户项目中建立customer sink class流程

- clone wormhole github 项目

- 安装包到本地仓库

- - wormhole/目录下执行mvn clean install -Pwormhole

- 添加依赖

- - 如果使用sparkx则添加对sparkx的依赖

 <dependency>

​     <groupId>edp.wormhole</groupId>

​     <artifactId>wormhole-sinks</artifactId>

​     <version>0.6.3</version>

  </dependency>

- - 如果使用flinkx则添加对flinkx的依赖	

<dependency>

​            <groupId>edp.wormhole</groupId>

​            <artifactId>wormhole-ums_1.3-flinkx_1.5.1</artifactId>

​            <version>0.6.3</version>

 </dependency>

- 新建customer sink class，该class需要继承edp.wormhole.publicinterface.sinks.SinkProcessor，并实现process方法
- 用户项目打包：需要打全量包，即包含sparkx或者flinkx包或者中全部的依赖
- 上传用户jar：将用户jar包放置到wormhole项目下的app/目录中
- 配置application.conf文件：设置spark.wormhole.jar.path参数设置为用户jar包名称

2、配置flow

配置flow在Sink Config中配置customer sink class的完整的名字

{"other_sinks_config":{"customer_sink_class_fullname":"customer sink full class name"}}

### Transformation

#### Spark Flow Transformation

配置数据转换逻辑，支持 SQL 和自定义 Class 方式，可以配置多条转换逻辑，调整逻辑顺序

##### 自定义 Class

- pom中添加 wormhole/sparkxinterface 依赖

  ```
  <dependency>
     <groupId>edp.wormhole</groupId>
     <artifactId>wormhole-sparkxinterface</artifactId>
     <version>0.6.3</version>
  </dependency>
  ```

- clone wormhole github 项目，本地安装 wormhole-sparkxinterface jar 包

  ```

  安装wormhole-sparkxinterface包至本地maven仓库

  wormhole/util目录下执行

  mvn clean install package

  wormhole/ums目录下执行

  mvn clean install package

  wormhole/interface/sparkxinterface目录下执行

  mvn clean install package
  ```

- 继承 并实现 wormhole/interface/sparkxinterface module 下的 edp.wormhole.sparkxinterface.swifts.SwiftsInterface 接口，可参考 wormhole/sparkx module 下的 edp.wormhole.sparkx.swifts.custom.CustomTemplate 类

- 编译打包，将带有 Dependencies 的 Jar 包放置在 $SPARK_HOME/jars 目录下

- 页面配置时，选择 Custom Class，输入方法名全路径，如 edp.wormhole.sparkx.swifts.custom.CustomTemplate

- Flow 启动或生效，重启 Stream

##### SQL

###### Lookup SQL

Lookup SQL 可以关联流下其他系统数据，如 RDBS/Hbase/Redis/Elasticsearch 等，规则如下。

若 Source Namespace 为 kafka.edp_kafka.udftest.udftable，Lookup Table 为 RDBMS 系统，如 mysql.er_mysql.eurus_test 数据库下的 eurus_user 表，Left Join 关联字段是 id，name，且从 Lookup 表中选择的字段 id，name 与主流上kafka.edp_kafka.udftest.udftable 中的字段重名，0.6.0及以上版本支持两种类型的Lookup SQL语句如下：

（1）主流上的字段名用${}标注（0.6.0及以上版本支持），推荐使用该种方式，例如

```
select id as id1,name as name1,address,age from eurus_user where (id,name) in (${id},${name});
```

（2）主流上的字段名用namespace.filedName进行标注，例如

```
select id as id1, name as name1, address, age from eurus_user where (id, name) in (kafka.edp_kafka.udftest.udftable.id, kafka.edp_kafka.udftest.udftable.name);
```

（3）关系型数据库支持不关联流上字段进行join（0.6.3及之后版本支持），例如

```
select id as id1, name as name1, address, age from eurus_user where id = 1;
```

这种方式要慎用，如果流上数据为n条，从数据库里查出来是m条，那么join之后数据的总量就会是n*m条，可能会造成内存溢出。

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
- select 语句规则同 Lookup SQL


#### Flink Flow Transformation

配置数据转换逻辑，支持 SQL ，可以配置多条转换逻辑，调整逻辑顺序。

支持两种事件模型Processing Time和Event Time。Processing Time为数据处理时的时间，即数据进入flink operator时获取时间戳；Event Time为事件产生的时间，即数据产生时自带时间戳，在Wormhole系统中对应```ums_ts_```字段。

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

- Agg：将匹配的多条数据做聚合，生成一条数据输出,例：field1:avg,field2:max（目前支持max/min/avg/sum/count，count为0.6.0版本新增功能）
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

Lookup SQL具体可参考Spark Flow Transformation的Lookup SQL章节

Flink SQL 用于处理 Source Namespace 数据，from 后面直接接表名即可。Wormhole 0.6.0-beata及之后版本的Flinkx支持window，UDF和UDAF操作。0.6.0版本Flink SQL支持key by操作，key by字段在Transformation Config中进行配置，设置格式为json，其中json中key为key_by_fields，value为key by的字段，如果有多个字段，则用逗号分隔，例如：{"key_by_fields":"name,city"}

###### Window

process time处理方式中window中相应的字段名称为processing_time。例：SELECT name, SUM(key) as keysum from ums GROUP BY TUMBLE(processing_time, INTERVAL '1' HOUR), name;

event time处理方式中window中相应的字段名称为ums_ts_。例：SELECT name, SUM(key) as keysum from ums GROUP BY TUMBLE(ums_ts_, INTERVAL '1' HOUR), name;

相关配置包括：

- min_idle_state_retention_time：聚合相关key值状态被保留的最短时间，默认12hours

- max_idle_state_retention_time：聚合相关key值状态被保留的最长时间，默认24hours

- preserve_message_flag：table to stream的转换采用的是retractStream，用户可以选择是否保留数据流中的message_flag字段。如果不保留，该参数配置为false，Wormhole会去掉message_flag字段；若保留，改参数配置为true，Wormhole会在Row中增加一个field，用来保存message_flag字段。默认为false

  在Transformation Config中可对这三个参数进行配置，配置格式为json。例如：{"min_idle_state_retention_time":"10","max_idle_state_retention_time":"20","preserve_message_flag":"true"}



###### UDF

Wormhole Flink UDF支持普通的java程序，而不需要按照Flink官方文档的格式实现UDF。UDF名称大小写敏感。UDF相应的字段需要使用as指定新字段的名称。例如：

Java程序：

    public class addint {
      public int fInt(int i) {
          return i + 1;
      }
    }
使用UDF的Flink SQL：

    select intvalue, fInt(intvalue) as fint from mytable; 


###### UDAF

（1）使用UDAF需要进行以下操作

- pom中添加 wormhole/flinkxinterface 依赖

  ```
  <dependency>
     <groupId>edp.wormhole</groupId>
     <artifactId>wormhole-flinkxinterface</artifactId>
     <version>0.6.3</version>
  </dependency>
  ```

- clone wormhole github 项目，本地安装 wormhole-flinkxinterface jar 包

  ```
  安装wormhole-flinkxinterface包至本地maven仓库

  wormhole/interface/flinkxinterface目录下执行

  mvn clean install package
  ```

- 继承 并实现 wormhole/interface/flinkxinterface module 下的 edp.wormhole.flinkxinterface.UdafInterface 接口。

- 编译打包，将带有 Dependencies 的 Jar 包放置在 $FLINK_HOME/lib 目录下

- 页面配置时，在admin用户下进行注册。

- 重启 Stream，flow启动时，选择该UDAF。

（2）UDAF例程：计算带权重的值的平均值

- 首先需要定义一个累加器，该累加器是用来保存聚合的中间结果的数据结构

  ```
  public class WeightedAvgAccum {
      public long sum = 0;
      public int count = 0;
      public WeightedAvgAccum(long sum, int count) {
          this.sum = sum;
          this.count = count;
      }
  }
  ```

- 创建聚合函数

  - 覆盖createAccumulator()方法，通过该方法创建空的累加器
  - 实现accumulate()方法（不需要进行覆盖），每个输入通过该方法更新累加器
  - 覆盖getValue()方法，在处理完所有行之后，调用该方法计算并返回最终结果
  - 对于over window需要实现retract()方法（不需要进行覆盖），否则可不实现该方法
  - 对于session window需要覆盖merge()方法，否则可不实现该方法

  ```
  public class udafAvg extends UdafInterface <Long, WeightedAvgAccum> {
      //创建空累加器
      @Override
      public WeightedAvgAccum createAccumulator() {
          return new WeightedAvgAccum(0, 0);
      }
      //更新累加器
      public void accumulate(WeightedAvgAccum acc, long value, int weight) {
          acc.sum += Long.valueOf(String.valueOf(value)) * Integer.valueOf(String.valueOf(weight));
          acc.count += Integer.valueOf(String.valueOf(weight));
      }
      //计算结果
      @Override
      public Long getValue(WeightedAvgAccum acc) {
          if (acc.count == 0) {
              return null;
          } else {
              return acc.sum / acc.count;
          }
      }
  }
  ```

（3）使用UDAF的Flink SQL：

```
SELECT name, udafAvg(score,weight) as udafAvg from ums GROUP BY name;
```

##### 异常处理设置

Flink中通过Transformation Config可选择对流处理中异常信息的处理方式。现在能捕获读取kafka后数据预处理、lookup操作、写sink时的异常。处理方式有三种：

- 不设置或者设置为unhandle：对捕获的异常信息不进行处理，只显示在log中

- 设置为interrupt：捕获到异常后，中断处理

- 设置为feedback：将捕获到的异常回灌到kafka中

  设置格式为json，例如：{"exception_process_method":"interrupt"}

**注意：当在配置文件中设置checkpoint为true，则异常处理不能设置为interrupt，否则flow会一直重启。**

### 修改 Flow

修改 Flow 时，不能修改所选 Stream，SourceNamespace 和 SinkNamespace，可以修改 Protocol 类型，Result Fields，Sink Config 和 Transformation 转换逻辑

### 启动 Flow

#### 启动 Spark Flow

- Sink 端为 RDBMS 时，需将相应的 jdbc connector jar 放至 $SPARK_HOME/jars 目录下，然后启动或重启 Stream
- 点击启动按钮，后台会将 Flow 的信息提交给 Stream，且会将 Flow Source Namespace 和 Stream Join Namespaces 所在 Topic 绑定到 Stream 上
- Stream 接收到 Flow 指令后解析，若成功解析，返回成功信息，Flow 的状态由 starting 转成 running；若解析失败，Flow 的状态由 starting 转成 failed。可在 Yarn 上查看 Driver 日志，根据错误提示重新配置 Flow

#### 启动 Flink Flow

- Stream running状态下才可以启动Flow
- 启动 Flow 时可以选择需要加载的 UDF，也可以取消已选择的 UDF
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

####  Flink Error列表

可通过error列表查看失败数据的offset，并针对失败数据提交backfill作业

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

首先使用 hdfslog Stream 将源数据备份到 Hdfs，Flow 出错或需要重算时，可配置 Job 重算。具体配置可参考Stream 和 Flow。Job中source端可选择数据的版本信息，将该版本的数据重算。

Job配置中version为namespace的第五层，表示数据的版本。使用 hdfslog Stream 将源数据备份到 Hdfs时，改层需要为数字，配置job时，可根据不同的版本进行数据重算。

Job中Spark SQL表名为“increment”。例如：

`select key, value from increment;`

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
