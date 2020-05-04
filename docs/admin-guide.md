---
layout: global
displayTitle: Admin Guide
title: Admin Guide
description: Wormhole Admin Guide page

---

* This will become a table of contents (this text will be scraped).
{:toc}
Wormhole 系统中有三类用户角色 Admin，User，App。本章介绍 Admin 类型用户的使用规范。

## User 管理

### Admin

管理员，具有管理 User，Namespace，UDF，Project 的权限，只有查看 Stream，Flow，Job 的权限。

Admin 配置好 User，Namespace，UDF 和 Project 的访问权限后，User 类型的用户便可以通过配置 Stream，Flow，Job 处理自身有权限访问的数据。

### User

普通用户，具有管理 Stream，Flow，Job 的权限，可以使用 Admin 授权其访问的 Project 下的 Namespace，UDF 等。

### App

第三方系统用户，可通过 Wormhole 提供的 Restful API 交互，具有 Admin 和 User 的部分权限。

接口具体信息可查看 http://ip:port/swagger文档页面的 app 部分。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-user-list.png" alt="" width="600"/>

## Namespace 管理

Source/Sink/Lookup Namespace管理，其中Source Namespace有两种来源：DBus已接Namespace资源同步，参考[DBus对接](https://edp963.github.io/wormhole/how-to.html#dbus%E7%B3%BB%E7%BB%9F%E5%AF%B9%E6%8E%A5)，Kafka类型的数据源。

目前只须在 Wormhole 上配置前四部分，Table Version/Database Partition/Table Partition 默认为 “*”。

数据源系统只支持 Kafka，目标端系统支持 Kafka/RDBS/Elasticsearch/Hbase/Phoenix/Cassandra/MongoDB/Kudu等。

数据源表，目标表及 Flow 处理逻辑中需要关联的表都需要在 Wormhole 上配置。

**配置时按照层级，先配置 Instance，然后配置 Database 时选择已有的 Instance，最后配置 Namespace 时选择已有的 Instance 和 Database，Namespace 相当于 Table。**

### Instance

instance 相当于为数据系统的物理地址起别名，connUrl 填写规则可通过点击帮助按钮查看。

新建 instance 后，instance 值不可以修改，connUrl 可以修改。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-instance.png" alt="" width="600"/>

备注：
(1)如果kafka为Kerberos认证的kafka，则将Connection Config设置为{"kerberos":true}，否则配置为{"kerberos":false}，如果没有配置，默认为application.conf中设置的Kerberos信息（0.6.3及之后版本支持）。
(2)es 如果是sink es，则Connection URL需要在前面加上http://ip:port（port为http访问端口）；如果是lookup es，则Connection URL为ip:port（port为jdbc访问端口），config中需要配置cluster.name=名字
### Database

database 配置数据库名，用户名，密码及连接配置等信息。

如 mysql 的数据库连接配置可填写在 config 项中。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-database.png" alt="" width="600"/>

### Namespace

namespace 页面选择已有的 instance和 database，配置表名和主键，一次可配置多张表。主键大小写敏感，若源数据来源为 Dbus，主键须配置为小写。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-namespace.png" alt="" width="600"/>

**若 Namespace 的系统类型为 Kafka，且在 Wormhole 中作为 Source Namespace，若数据格式为UMS_Extension，可配置 Namespace 的 schema。**

namespace 系统类型为 Kafka 时，若其作为 Source Namespace，且数据格式为 UMS_Extension，可配置 source schema。 

namespace 系统类型为 Kafka/MongoDb/Es 时，若其作为 Sink Namespace，且输出的格式中有嵌套类型，可配置 sink schema。 

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-namespace-schema.png" alt="" width="600"/>

#### Key

支持联合主键配置，中间使用逗号分隔。Key 区分大小写，若数据来源为 Dbus，主键须配置为小写。

#### Source Schema 

点击 namespace source schema 配置按钮，见下图，粘贴一条完整的数据样例至左侧文本框中，点击中间的按钮，生成右侧的配置表格。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-namespace-sourceschema.png" alt="" width="600"/>

UMS_Extension 格式支持的字段类型如下。

- 基本类型：int/long/float/double/decimal/string/boolean/datetime/binary。
- 基本类型衍生的数组类型：intarray/longarray/floatarray/doublearray/decimalarray/stringarray/booleanarray/datetimearray/binaryarray。其中 intarray 对应的数据格式为 [1,2,3]。
- json 类型：jsonobejct/jsonarray。jsonobject 对应的数据格式为 `{"id": 1}`，jsonarray 对应的数据格式为 `[{"id": 1}]`。
- tuple 类型：tuple。tuple 对应的数据格式为 `"2017-06-27 14:14:04,557 |INFO"`，数据中存在特殊字符，并且处理数据时需要以特殊字符作为分隔符处理，可在页面上配置 tuple 的分隔符和分割出的字段个数，并为每个字段命名。

**注意事项：**

> - **ums_ts_ 对应字段必须配置，且数据类型须为 long/datetime/longarray/datetimearray，若为datetime/datetimearray 类型，数据格式须为 yyyy/MM/dd HH:mm:ss[.SSS000/SSS] 或 yyyy-MM-dd HH:mm:ss[.SSS000/SSS] 或 yyyyMMddHHmmss[SSS000/SSS]。若ums_ts_字段设置为long类型，对应数值应该为时间对应的秒数或毫秒数**
> - **ums_id_ 对应字段的数据类型须为 int/long/intarray/longarray**
> - **ums_op_ 对应字段的数据类型须为 int/long/string/intarray/longarray/stringarray，配置 insert 操作，update 操作，delete 操作对应的值**
> - **如果向 sink 端写数据时只有插入操作，可不配置 ums_id_ 和 ums_op_ 系统字段；如果有增删改操作，必须配置**
> - **tuple 类型只能有一个**
> - **source schema 中只能包含一个array 类型（intarray 等或 jsonarray），且须将其或其子字段配置为主键之一。**

#### Sink Schema

点击 namespace sink schema 配置按钮，见下图，粘贴一条完整的数据样例至右侧文本框中，点击中间的反推按钮，生成左侧的配置表格。支持的字段类型见 source schema，不支持 tuple 类型。

**注意事项：若需要输出 `ums_id_,ums_ts_,ums_active_` 系统字段，须在数据样例中添加相应字段。**

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-namespace-sinkschema.png" alt="" width="600"/>

## UDF 管理

Spark SQL 支持 UDF(User Define Function) 功能，用户可以在 Spark SQL 里自定义函数来处理数据，Wormhole 有提供 UDF 管理功能。

UDF 只支持 Java 语言编写，编译打包后将 Jar 包上传至 application.conf 中 spark.wormhole.hdfs.root.path 配置项对应 hdfs 目录的 /udfjars 子目录下。

UDF 由 Admin 统一管理，有公有、私有两种类型，所有 Project 都可以访问公有 UDF，私有 UDF 需要 Admin 授权。

配置 UDF 时，输入方法名，方法名与 Class 中的 Function 名称保持一致，类名全称，Jar 包名称，保存时会检查 Hdfs 目录上是否存在该 Jar 包，不存在则无法新建。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-udf.png" alt="" width="600"/>

## Project 管理

Admin 配置好 Namespace/User/UDF 后，可创建 Project，为其分配 Namespace/User/UDF，设置该Project 下的 Stream 最多可使用的内存大小和 Vcore 个数。

点击 Project 图标，可查看该 Project 下 Namespace/User/UDF/Stream/Flow/Job 详情。

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin-project.png" alt="" width="600"/>

## Stream 管理

Admin 用户只有查看 Stream 详情的权限，可点击 Stream 导航栏查看所有 Stream 信息，也可以进入到某个Project 下，查看该 Project 对应的 Stream 信息。

## Flow 管理

Admin 用户只有查看 Flow 详情的权限，可点击 Flow 导航栏查看所有 Flow 信息，也可以进入到某个 Project 下，查看该 Project 对应的 Flow 信息。

## Job 管理

Admin 用户只有查看 Job 详情的权限，可点击 Job 导航栏查看所有 Job 信息，也可以进入到某个 Project 下，查看该 Project 对应的 Job 信息。