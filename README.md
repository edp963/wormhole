

## What is Wormhole?

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![Build Status](https://travis-ci.org/edp963/wormhole.svg?branch=master)](https://travis-ci.org/edp963/wormhole)
[![Coverage Status](https://coveralls.io/repos/github/edp963/wormhole/badge.svg)](https://coveralls.io/github/edp963/wormhole)

> 来自[宜信](https://www.creditease.cn/)[技术研发中心](http://crdc.creditease.cn/)的流式处理平台

Wormhole是一个一站式流式处理云平台解决方案（SPaaS - Stream Processing as a Service）。

Wormhole面向大数据流式处理项目的开发管理运维人员，致力于提供统一抽象的概念体系，直观可视化的操作界面，简单流畅的配置管理流程，基于SQL即可完成的业务逻辑开发方式，并且屏蔽了流式处理的底层技术细节，极大的降低了数据项目管理运维门槛，使得大数据流式处理项目的开发管理运维变得更加轻量敏捷可控可靠。

## Architecture

<img src="https://github.com/edp963/wormhole/raw/master/docs/img/wh4_pipeline_overview.png" alt="" width="600"/>

## Experience Preview

#### Admin可以创建Project/Namespace/User/UDF，并且可以查看所有Flow/Stream/Job
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_1.png" alt="" width="600"/>

#### Admin可以为Project分配Namespace资源/User资源/UDF资源/计算资源，以支持多租户资源隔离
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/admin_2.png" alt="" width="600"/>

#### User可以对自己有权限的Project进行开发实施和管理运维工作
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_1_project.png" alt="" width="600"/>

#### User可以通过简单配置步骤即可搭建起一个流式作业pipeline（Flow），只需关注数据从哪来到哪去如何转换
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_1.png" alt="" width="600"/>

#### 转换支持大部分流上作业常用场景，大部分工作可以通过配置SQL实现流上处理逻辑
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_2_flow_2.png" alt="" width="600"/>

#### Wormhole有Flow和Stream的概念，支持在一个物理Stream（对应一个Spark Streaming作业）里通过处理多个逻辑Flow，使得User可以更加精细灵活的利用计算资源
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_3_stream.png" alt="" width="600"/>

#### Wormhole也支持批处理Job，同样可以配置化实现处理逻辑并落到多个异构Sink，Flow和Job的配合可以很容易实现Lambda架构和Kappa架构
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_1.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_4_job_2.png" alt="" width="600"/>

#### User可以查看Project相关的Namespace/User/UDF/Resource
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_ns.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_5_user.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_6_udf.png" alt="" width="600"/>
<img src="https://github.com/edp963/wormhole/raw/master/docs/img/user_7_res.png" alt="" width="600"/>

#### 以上是简短的功能和用户体验预览，更多强大的细节功能请参见Documentation

## Documentation

Please refer to [Wormhole用户手册](https://edp-wormhole.gitbooks.io/wormhole-user-guide-cn/content), or download [PDF](https://www.gitbook.com/download/pdf/book/edp-wormhole/wormhole-user-guide-cn).

## Latest Release

Please download the latest [RELEASE](https://github.com/edp963/wormhole/releases/download/0.3.0/wormhole-0.3.0.tar.gz).

## Get Help

- **Mailing list**: edp_support@groups.163.com
- **WeChat**: edpstack <img src="https://github.com/edp963/edp-resource/raw/master/WeChat.jpg" alt="" width="100"/>

## License

Wormhole is under the Apache 2.0 license. See the [LICENSE](https://github.com/edp963/wormhole/blob/master/LICENSE) file for details.
