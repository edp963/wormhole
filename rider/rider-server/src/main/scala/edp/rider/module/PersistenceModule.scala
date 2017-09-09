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

import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.base._
import edp.rider.rest.persistence.dal._
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
    try {
      sqlSeq.filter(_.toLowerCase().contains("create")).map(session.withPreparedStatement(_)(_.execute))
    } catch {
      case ex: Exception => riderLogger.warn("create table exception", ex)
    }
    finally {
      session.close()
    }
  }

}

trait PersistenceModule {

  val instanceDal: BaseDal[InstanceTable, Instance]
  val databaseDal: NsDatabaseDal
  val namespaceDal: NamespaceDal
  val userDal: UserDal
  val relProjectUserDal: RelProjectUserDal
  val streamDal: StreamDal
  val flowDal: FlowDal
  val relProjectNsDal: RelProjectNsDal
  val projectDal: ProjectDal
  val dbusDal: BaseDal[DbusTable, Dbus]
  val directiveDal: BaseDal[DirectiveTable, Directive]
  val inTopicDal: BaseDal[StreamInTopicTable, StreamInTopic]

  val jobDal: JobDal

  val feedbackHeartbeatDal: BaseDal[FeedbackHeartbeatTable, FeedbackHeartbeat]
  val feedbackOffsetDal: FeedbackOffsetDal
  val feedbackStreamErrDal: BaseDal[FeedbackStreamErrTable, FeedbackStreamErr]
  val feedbackFlowErrDal: FeedbackFlowErrDal
  val feedbackDirectiveDal: BaseDal[FeedbackDirectiveTable, FeedbackDirective]

  val instanceQuery: TableQuery[InstanceTable] = TableQuery[InstanceTable]
  val databaseQuery: TableQuery[NsDatabaseTable] = TableQuery[NsDatabaseTable]
  val namespaceQuery: TableQuery[NamespaceTable] = TableQuery[NamespaceTable]
  val userQuery = TableQuery[UserTable]
  val relProjectUserQuery = TableQuery[RelProjectUserTable]
  val streamQuery = TableQuery[StreamTable]
  val flowQuery = TableQuery[FlowTable]
  val relProjectNsQuery = TableQuery[RelProjectNsTable]
  val projectQuery = TableQuery[ProjectTable]
  val dbusQuery = TableQuery[DbusTable]
  val directiveQuery = TableQuery[DirectiveTable]
  val streamInTopicQuery = TableQuery[StreamInTopicTable]

  val jobQuery = TableQuery[JobTable]

  val feedbackHeartBeatQuery = TableQuery[FeedbackHeartbeatTable]
  val feedbackOffsetQuery = TableQuery[FeedbackOffsetTable]
  val feedbackStreamErrQuery = TableQuery[FeedbackStreamErrTable]
  val feedbackFlowErrQuery = TableQuery[FeedbackFlowErrTable]
  val feedbackDirectiveQuery = TableQuery[FeedbackDirectiveTable]

}

trait PersistenceModuleImpl extends PersistenceModule {
  this: ConfigurationModule =>

  override lazy val instanceDal = new BaseDalImpl[InstanceTable, Instance](instanceQuery)
  override lazy val databaseDal = new NsDatabaseDal(databaseQuery, instanceQuery)
  override lazy val namespaceDal = new NamespaceDal(namespaceQuery, databaseDal, instanceDal, dbusDal)
  override lazy val userDal = new UserDal(userQuery, relProjectUserDal)
  override lazy val relProjectUserDal = new RelProjectUserDal(userQuery, projectQuery, relProjectUserQuery)
  override lazy val streamDal = new StreamDal(streamQuery, projectQuery, feedbackOffsetQuery, instanceQuery, databaseQuery, relProjectNsQuery, streamInTopicQuery, namespaceQuery, dbusQuery, directiveDal)
  override lazy val flowDal = new FlowDal(flowQuery, streamQuery, projectQuery, streamDal)
  override lazy val relProjectNsDal = new RelProjectNsDal(namespaceQuery, databaseQuery, instanceQuery, projectQuery, relProjectNsQuery, streamInTopicQuery)
  override lazy val projectDal = new ProjectDal(projectQuery, relProjectNsDal, relProjectUserDal)
  override lazy val dbusDal = new BaseDalImpl[DbusTable, Dbus](dbusQuery)
  override lazy val directiveDal = new BaseDalImpl[DirectiveTable, Directive](directiveQuery)
  override lazy val inTopicDal = new BaseDalImpl[StreamInTopicTable, StreamInTopic](streamInTopicQuery)

  override lazy val jobDal = new JobDal(jobQuery)

  override lazy val feedbackHeartbeatDal = new BaseDalImpl[FeedbackHeartbeatTable, FeedbackHeartbeat](feedbackHeartBeatQuery)
  override lazy val feedbackOffsetDal = new FeedbackOffsetDal(feedbackOffsetQuery)
  override lazy val feedbackStreamErrDal = new BaseDalImpl[FeedbackStreamErrTable, FeedbackStreamErr](feedbackStreamErrQuery)
  override lazy val feedbackFlowErrDal = new FeedbackFlowErrDal(feedbackFlowErrQuery)
  override lazy val feedbackDirectiveDal = new BaseDalImpl[FeedbackDirectiveTable, FeedbackDirective](feedbackDirectiveQuery)

}
