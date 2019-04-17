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


package edp.rider.module

import java.sql.SQLException

import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.base._
import edp.rider.rest.persistence.dal.{MonitorInfoDal, _}
import edp.rider.rest.persistence.entities._
import slick.basic.DatabaseConfig
import slick.jdbc.JdbcProfile
import slick.lifted.TableQuery

import scala.io.Source

object DbModule extends ConfigurationModuleImpl with RiderLogger {
  private lazy val dbConfig: DatabaseConfig[JdbcProfile] = DatabaseConfig.forConfig("mysql", config)

  lazy val sqlSeq = Source.fromFile(s"${RiderConfig.riderRootPath}/conf/wormhole.sql").mkString.split(";")

  lazy val profile: JdbcProfile = dbConfig.profile
  lazy val db: JdbcProfile#Backend#Database = dbConfig.db

  def createSchema: Unit = {
    val session = db.createSession()
    sqlSeq.filter(sql =>
      sql.trim.toLowerCase().startsWith("create")
        || sql.trim.toLowerCase().startsWith("alter")
        || sql.trim.toLowerCase().startsWith("drop"))
      .map { sql =>
        try {
          session.withPreparedStatement(sql)(_.execute)
        } catch {
          case _: SQLException =>
        }
      }
    riderLogger.info("Initial rider database success")
    session.close()
  }

}

trait PersistenceModule {

  val instanceDal: InstanceDal
  val databaseDal: NsDatabaseDal
  val namespaceDal: NamespaceDal
  val userDal: UserDal
  val relProjectUserDal: RelProjectUserDal
  val streamDal: StreamDal
  val flowDal: FlowDal
  val flowHistoryDal: FlowHistoryDal
  val relProjectNsDal: RelProjectNsDal
  val projectDal: ProjectDal
  val dbusDal: DbusDal
  val directiveDal: DirectiveDal
  val streamInTopicDal: StreamInTopicDal
  val flowInTopicDal: FlowInTopicDal
  val streamUdfTopicDal: StreamUserDefinedTopicDal
  val flowUdfTopicDal: FlowUserDefinedTopicDal
  val flowUdfDal: FlowUdfDal

  val jobDal: JobDal
  val udfDal: UdfDal
  val relProjectUdfDal: RelProjectUdfDal
  val relStreamUdfDal: RelStreamUdfDal
  val feedbackStreamErrDal: FeedbackStreamErrorDal
  val feedbackFlowErrDal: FeedbackFlowErrDal
  val feedbackHeartbeatDal: FeedbackHeartbeatDal
  val feedbackOffsetDal: FeedbackOffsetDal
  val feedbackDirectiveDal: BaseDal[FeedbackDirectiveTable, FeedbackDirective]
  val feedbackErrDal: FeedbackErrDal
  val monitorInfoDal: MonitorInfoDal
  val rechargeResultLogDal: BaseDal[RechargeResultLogTable, RechargeResultLog]

  val instanceQuery: TableQuery[InstanceTable] = TableQuery[InstanceTable]
  val databaseQuery: TableQuery[NsDatabaseTable] = TableQuery[NsDatabaseTable]
  val namespaceQuery: TableQuery[NamespaceTable] = TableQuery[NamespaceTable]
  val userQuery = TableQuery[UserTable]
  val relProjectUserQuery = TableQuery[RelProjectUserTable]
  val streamQuery = TableQuery[StreamTable]
  val flowQuery = TableQuery[FlowTable]
  val flowHistoryQuery = TableQuery[FlowHistoryTable]
  val relProjectNsQuery = TableQuery[RelProjectNsTable]
  val projectQuery = TableQuery[ProjectTable]
  val dbusQuery = TableQuery[DbusTable]
  val directiveQuery = TableQuery[DirectiveTable]
  val streamInTopicQuery = TableQuery[StreamInTopicTable]
  val flowInTopicQuery = TableQuery[FlowInTopicTable]
  val udfTopicQuery = TableQuery[StreamUserDefinedTopicTable]
  val flowUdfTopicQuery = TableQuery[FlowUserDefinedTopicTable]
  val flowUdfQuery = TableQuery[FlowUdfTable]
  val jobQuery = TableQuery[JobTable]
  val udfQuery = TableQuery[UdfTable]
  val relProjectUdfQuery = TableQuery[RelProjectUdfTable]
  val relStreamUdfQuery = TableQuery[RelStreamUdfTable]
  val feedbackStreamErrQuery = TableQuery[FeedbackStreamErrTable]
  val feedbackFlowErrQuery = TableQuery[FeedbackFlowErrTable]
  val feedbackHeartBeatQuery = TableQuery[FeedbackHeartbeatTable]
  val feedbackOffsetQuery = TableQuery[FeedbackOffsetTable]
  val feedbackDirectiveQuery = TableQuery[FeedbackDirectiveTable]
  val feedbackErrQuery = TableQuery[FeedbackErrTable]
  val monitorInfoQuery = TableQuery[MonitorInfoTable]
  val rechargeResultLogQuery = TableQuery[RechargeResultLogTable]

}

trait PersistenceModuleImpl extends PersistenceModule {
  this: ConfigurationModule =>

  override lazy val instanceDal = new InstanceDal(instanceQuery, databaseDal)
  override lazy val databaseDal = new NsDatabaseDal(databaseQuery, instanceQuery)
  override lazy val namespaceDal = new NamespaceDal(namespaceQuery, databaseDal, instanceDal, dbusDal)
  override lazy val userDal = new UserDal(userQuery, relProjectUserDal, projectDal)
  override lazy val relProjectUserDal = new RelProjectUserDal(userQuery, projectQuery, relProjectUserQuery)
  override lazy val relStreamUdfDal = new RelStreamUdfDal(relStreamUdfQuery, udfQuery)
  override lazy val streamDal = new StreamDal(streamQuery, instanceDal, streamInTopicDal, relStreamUdfDal, projectQuery)
  override lazy val flowDal = new FlowDal(flowQuery, streamQuery, projectQuery, streamDal, streamInTopicDal, flowInTopicDal, flowUdfTopicDal, flowHistoryDal)
  override lazy val flowHistoryDal = new FlowHistoryDal(flowHistoryQuery)
  override lazy val relProjectNsDal = new RelProjectNsDal(namespaceQuery, databaseQuery, instanceQuery, projectQuery, relProjectNsQuery, streamQuery)
  override lazy val projectDal = new ProjectDal(projectQuery, relProjectNsDal, relProjectUserDal, relProjectUdfDal, streamDal)
  override lazy val dbusDal = new DbusDal(dbusQuery)
  override lazy val directiveDal = new DirectiveDal(directiveQuery)
  override lazy val streamInTopicDal = new StreamInTopicDal(streamInTopicQuery, databaseQuery, feedbackOffsetQuery)
  override lazy val flowInTopicDal = new FlowInTopicDal(flowInTopicQuery, databaseQuery, feedbackOffsetQuery)
  override lazy val streamUdfTopicDal: StreamUserDefinedTopicDal = new StreamUserDefinedTopicDal(udfTopicQuery, streamQuery, instanceQuery)
  override lazy val flowUdfTopicDal: FlowUserDefinedTopicDal = new FlowUserDefinedTopicDal(flowUdfTopicQuery, flowQuery, instanceQuery)
  override lazy val flowUdfDal: FlowUdfDal = new FlowUdfDal(udfQuery, relProjectUdfDal, flowUdfQuery, projectDal, streamDal)
  override lazy val jobDal = new JobDal(jobQuery, projectQuery)
  override lazy val udfDal = new UdfDal(udfQuery, relProjectUdfDal, relStreamUdfDal, projectDal, streamDal)
  override lazy val relProjectUdfDal = new RelProjectUdfDal(udfQuery, projectQuery, relProjectUdfQuery)
  override lazy val feedbackStreamErrDal = new FeedbackStreamErrorDal(feedbackStreamErrQuery, streamDal)
  override lazy val feedbackFlowErrDal = new FeedbackFlowErrDal(feedbackFlowErrQuery, streamDal, flowDal)
  override lazy val feedbackHeartbeatDal = new FeedbackHeartbeatDal(feedbackHeartBeatQuery, streamDal)
  override lazy val feedbackOffsetDal = new FeedbackOffsetDal(feedbackOffsetQuery)
  override lazy val feedbackDirectiveDal = new BaseDalImpl[FeedbackDirectiveTable, FeedbackDirective](feedbackDirectiveQuery)
  override lazy val feedbackErrDal= new FeedbackErrDal(feedbackErrQuery)
  override lazy val monitorInfoDal = new MonitorInfoDal(monitorInfoQuery, streamDal, flowDal)
  override lazy val rechargeResultLogDal= new BaseDalImpl[RechargeResultLogTable,RechargeResultLog](rechargeResultLogQuery)
}
