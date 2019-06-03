/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package edp.rider.rest.persistence.entities

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable, SimpleBaseEntity}
import slick.lifted.{Rep, Tag}
import slick.jdbc.MySQLProfile.api._

case class SparkConfig(
                        JVMDriverConfig: Option[String] = None,
                        JVMExecutorConfig: Option[String] = None,
                        othersConfig: Option[String] = None
                      )
case class UserTimeInfo(
                       createTime: String,
                       createBy: Long,
                       updateTime: String,
                       updateBy: Long
                     )

case class Job(id: Long,
               name: String, // 1
               projectId: Long, // 1
               sourceNs: String, // 1
               sinkNs: String, // 1
               jobType: String, //1
               sparkConfig: SparkConfig,
               startConfig: String,
               eventTsStart: String, // 1
               eventTsEnd: String,
               sourceConfig: Option[String],
               sinkConfig: Option[String],
               tranConfig: Option[String],
               tableKeys: Option[String],
               desc: Option[String] = None,
               status: String,
               sparkAppid: Option[String] = None,
               logPath: Option[String] = None,
               startedTime: Option[String] = None,
               stoppedTime: Option[String] = None,
               userTimeInfo: UserTimeInfo) extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class SimpleJob(name: String,
                     sourceNs: String,
                     sinkNs: String,
                     jobType: String,
                     sparkConfig: SparkConfig,
                     //JVMDriverConfig: Option[String] = None,
                     //JVMExecutorConfig: Option[String] = None,
                     //othersConfig: Option[String] = None,
                     startConfig: String,
                     eventTsStart: String,
                     eventTsEnd: String,
                     sourceConfig: Option[String],
                     sinkConfig: Option[String],
                     tranConfig: Option[String],
                     tableKeys: Option[String],
                     desc: Option[String] = None) extends SimpleBaseEntity

case class FullJobInfo(job: Job, projectName: String, disableActions: String)

case class JobTopicInfo(job: Job, projectName: String, topic: String, disableActions: String)

class JobTable(_tableTag: Tag) extends BaseTable[Job](_tableTag, "job") {
  def * = (id, name, projectId, sourceNs, sinkNs, jobType, (JVMDriverConfig, JVMExecutorConfig, othersConfig),
    startConfig, eventTsStart, eventTsEnd, sourceConfig, sinkConfig,
    tranConfig, tableKeys, desc, status, sparkAppid, logPath, startedTime, stoppedTime,
    (createTime, createBy, updateTime, updateBy)).shaped <> ({
    case (id, name, projectId, sourceNs, sinkNs, jobType, sparkConfig,
    startConfig, eventTsStart, eventTsEnd, sourceConfig, sinkConfig,
    tranConfig, tableKeys, desc, status, sparkAppid, logPath, startedTime, stoppedTime,
    userTimeInfo) =>
      Job(id, name, projectId, sourceNs, sinkNs, jobType, SparkConfig.tupled.apply(sparkConfig),
        startConfig, eventTsStart, eventTsEnd, sourceConfig, sinkConfig,
        tranConfig, tableKeys, desc, status, sparkAppid, logPath, startedTime, stoppedTime,
        UserTimeInfo.tupled.apply(userTimeInfo)
      )
  }, { j: Job =>
    def f1(s: SparkConfig) = SparkConfig.unapply(s).get
    def f2(c: UserTimeInfo) = UserTimeInfo.unapply(c).get
    Some((j.id, j.name, j.projectId, j.sourceNs, j.sinkNs, j.jobType, f1(j.sparkConfig),
      j.startConfig, j.eventTsStart, j.eventTsEnd, j.sourceConfig, j.sinkConfig,
      j.tranConfig, j.tableKeys, j.desc, j.status, j.sparkAppid, j.logPath, j.startedTime, j.stoppedTime,
      f2(j.userTimeInfo)))
  })

  /** Database column name SqlType(VARCHAR), Length(200,true) */
  val name: Rep[String] = column[String]("name", O.Length(200, varying = true))
  /** Database column project_id SqlType(BIGINT) */
  val projectId: Rep[Long] = column[Long]("project_id")
  /** Database column source_ns SqlType(VARCHAR), Length(200,true) */
  val sourceNs: Rep[String] = column[String]("source_ns", O.Length(200, varying = true))
  /** Database column sink_ns SqlType(VARCHAR), Length(200,true) */
  val sinkNs: Rep[String] = column[String]("sink_ns", O.Length(200, varying = true))
  /** Database column source_type SqlType(VARCHAR), Length(30,true) */
  val jobType: Rep[String] = column[String]("job_type", O.Length(30, varying = true))
  ///** Database column spark_config SqlType(VARCHAR), Length(1000,true), Default(None) */
  //val sparkConfig: Rep[Option[String]] = column[Option[String]]("spark_config", O.Length(5000, varying = true))
  /** Database column jvm_driver_config SqlType(VARCHAR), Length(1000,true) */
  val JVMDriverConfig: Rep[Option[String]] = column[Option[String]]("jvm_driver_config", O.Length(5000, varying = true))
  /** Database column jvm_executor_config SqlType(VARCHAR), Length(1000,true) */
  val JVMExecutorConfig: Rep[Option[String]] = column[Option[String]]("jvm_executor_config", O.Length(5000, varying = true))
  /** Database column others_config SqlType(VARCHAR), Length(1000,true) */
  val othersConfig: Rep[Option[String]] = column[Option[String]]("others_config", O.Length(5000, varying = true))
  /** Database column start_config SqlType(VARCHAR), Length(1000,true) */
  val startConfig: Rep[String] = column[String]("start_config", O.Length(1000, varying = true))
  /** Database column event_ts_start SqlType(VARCHAR), Length(50,true) */
  val eventTsStart: Rep[String] = column[String]("event_ts_start", O.Length(50, varying = true))
  /** Database column event_ts_end SqlType(VARCHAR), Length(50,true) */
  val eventTsEnd: Rep[String] = column[String]("event_ts_end", O.Length(50, varying = true))
  /** Database column source_config SqlType(VARCHAR), Length(5000,true) */
  val sourceConfig: Rep[Option[String]] = column[Option[String]]("source_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column sink_config SqlType(VARCHAR), Length(5000,true) */
  val sinkConfig: Rep[Option[String]] = column[Option[String]]("sink_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column tran_config SqlType(VARCHAR), Length(5000,true) */
  val tranConfig: Rep[Option[String]] = column[Option[String]]("tran_config", O.Length(5000, varying = true), O.Default(None))
  /** Database column table_keys SqlType(VARCHAR), Length(1000,true), Default(None) */
  val tableKeys: Rep[Option[String]] = column[Option[String]]("table_keys", O.Length(1000, varying = true), O.Default(None))
  /** Database column desc SqlType(VARCHAR), Length(1000,true), Default(None) */
  val desc: Rep[Option[String]] = column[Option[String]]("desc", O.Length(1000, varying = true), O.Default(None))
  /** Database column status SqlType(VARCHAR), Length(200,true) */
  val status: Rep[String] = column[String]("status", O.Length(200, varying = true))
  /** Database column spark_appid SqlType(VARCHAR), Length(200,true) */
  val sparkAppid: Rep[Option[String]] = column[Option[String]]("spark_appid", O.Length(200, varying = true), O.Default(None))
  /** Database column log_path SqlType(VARCHAR), Length(200,true) */
  val logPath: Rep[Option[String]] = column[Option[String]]("log_path", O.Length(200, varying = true), O.Default(None))
  /** Database column started_time SqlType(TIMESTAMP), Default(None) */
  val startedTime: Rep[Option[String]] = column[Option[String]]("started_time", O.Default(None))
  /** Database column stopped_time SqlType(TIMESTAMP), Default(None) */
  val stoppedTime: Rep[Option[String]] = column[Option[String]]("stopped_time", O.Default(None))

  /** Uniqueness Index over (sourceNs,sinkNs) (database name job_UNIQUE) */
  val index1 = index("name_UNIQUE", name, unique = true)
  val index2 = index("job_UNIQUE", (sourceNs, sinkNs), unique = true)
}
