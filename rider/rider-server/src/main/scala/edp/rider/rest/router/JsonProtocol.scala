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


package edp.rider.rest.router

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import edp.rider.common._
import edp.rider.rest.persistence.base.{BaseEntity, SimpleBaseEntity}
import edp.rider.rest.persistence.entities._
import spray.json.{DefaultJsonProtocol, JsValue, RootJsonFormat, _}

object JsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {

  implicit val formatLoginClass: RootJsonFormat[LoginClass] = jsonFormat2(LoginClass)
  implicit val formatSessionClass: RootJsonFormat[SessionClass] = jsonFormat4(SessionClass)
  implicit val formatChangeUserPwdClass: RootJsonFormat[ChangeUserPwdClass] = jsonFormat3(ChangeUserPwdClass)
  implicit val formatActionClass: RootJsonFormat[ActionClass] = jsonFormat2(ActionClass)
  implicit val formatResponseHeader: RootJsonFormat[ResponseHeader] = jsonFormat3(ResponseHeader)

  implicit def formatResponseJson[A: JsonFormat]: RootJsonFormat[ResponseJson[A]] = jsonFormat2(ResponseJson.apply[A])

  implicit def formatResponseSeqJson[A: JsonFormat]: RootJsonFormat[ResponseSeqJson[A]] = jsonFormat2(ResponseSeqJson.apply[A])

  implicit val formatInstance: RootJsonFormat[Instance] = jsonFormat10(Instance)
  implicit val formatSimpleInstance: RootJsonFormat[SimpleInstance] = jsonFormat4(SimpleInstance)
  implicit val formatDatabase: RootJsonFormat[NsDatabase] = jsonFormat14(NsDatabase)
  implicit val formatSimpleDatabase: RootJsonFormat[SimpleNsDatabase] = jsonFormat8(SimpleNsDatabase)
  implicit val formatDatabaseInstance: RootJsonFormat[DatabaseInstance] = jsonFormat17(DatabaseInstance)
  implicit val formatNamespace: RootJsonFormat[Namespace] = jsonFormat17(Namespace)
  implicit val formatNsTable: RootJsonFormat[NsTable] = jsonFormat2(NsTable)
  implicit val formatSimpleNamespace: RootJsonFormat[SimpleNamespace] = jsonFormat6(SimpleNamespace)
  implicit val formatNamespaceTopic: RootJsonFormat[NamespaceTopic] = jsonFormat18(NamespaceTopic)
  implicit val formatTransNamespace: RootJsonFormat[TransNamespace] = jsonFormat6(TransNamespace)
  implicit val formatNamespaceAdmin: RootJsonFormat[NamespaceAdmin] = jsonFormat19(NamespaceAdmin)
  implicit val formatSimpleDsDatabaseInstance: RootJsonFormat[NsDatabaseInstance] = jsonFormat7(NsDatabaseInstance)
  implicit val formatUser: RootJsonFormat[User] = jsonFormat10(User)
  implicit val formatSimpleUser: RootJsonFormat[SimpleUser] = jsonFormat4(SimpleUser)
  implicit val formatUserProject: RootJsonFormat[UserProject] = jsonFormat11(UserProject)
  implicit val formatProject: RootJsonFormat[Project] = jsonFormat11(Project)
  implicit val formatSimpleProject: RootJsonFormat[SimpleProject] = jsonFormat5(SimpleProject)
  implicit val formatRelProjectUser: RootJsonFormat[RelProjectUser] = jsonFormat8(RelProjectUser)
//  implicit val formatStreamUser: RootJsonFormat[Stream] = jsonFormat18(Stream)
  implicit val formatStreamTopic: RootJsonFormat[StreamTopic] = jsonFormat4(StreamTopic)
  implicit val formatStreamTopicTemp: RootJsonFormat[StreamTopicTemp] = jsonFormat5(StreamTopicTemp)
  implicit val formatStreamUdf: RootJsonFormat[StreamUdf] = jsonFormat4(StreamUdf)
  implicit val formatStreamUdfTemp: RootJsonFormat[StreamUdfTemp] = jsonFormat5(StreamUdfTemp)
  implicit val formatStreamZkUdfTemp: RootJsonFormat[StreamZkUdfTemp] = jsonFormat4(StreamZkUdfTemp)
  implicit val formatStreamZkUdf: RootJsonFormat[StreamZkUdf] = jsonFormat3(StreamZkUdf)
//  implicit val formatStreamDetail: RootJsonFormat[StreamDetail] = jsonFormat6(StreamDetail)
  implicit val formatFlow: RootJsonFormat[Flow] = jsonFormat16(Flow)
  implicit val formatSimpleFlow: RootJsonFormat[SimpleFlow] = jsonFormat7(SimpleFlow)
  implicit val formatFlowStream: RootJsonFormat[FlowStream] = jsonFormat21(FlowStream)
  implicit val formatFlowStreamAdmin: RootJsonFormat[FlowStreamAdmin] = jsonFormat22(FlowStreamAdmin)
  implicit val formatJobUser: RootJsonFormat[Job] = jsonFormat22(Job)
  implicit val formatSimpleJobUser: RootJsonFormat[SimpleJob] = jsonFormat11(SimpleJob)
  implicit val formatFullJobInfo: RootJsonFormat[FullJobInfo] = jsonFormat3(FullJobInfo)


  implicit val formatSimpleProjectRel: RootJsonFormat[SimpleProjectRel] = jsonFormat8(SimpleProjectRel)
  implicit val formatProjectUserNsUdf: RootJsonFormat[ProjectUserNsUdf] = jsonFormat14(ProjectUserNsUdf)
//  implicit val formatStreamProject: RootJsonFormat[StreamProject] = jsonFormat2(StreamProject)

//  implicit val formatSimpleStream: RootJsonFormat[SimpleStream] = jsonFormat10(SimpleStream)
  implicit val formatFlowStreamInfo: RootJsonFormat[FlowStreamInfo] = jsonFormat22(FlowStreamInfo)
//  implicit val formatStreamReturn: RootJsonFormat[StreamReturn] = jsonFormat2(StreamReturn)
//  implicit val formatStreamWithBrokers: RootJsonFormat[StreamWithBrokers] = jsonFormat2(StreamWithBrokers)
  implicit val formatStreamIntopic: RootJsonFormat[StreamInTopic] = jsonFormat11(StreamInTopic)
//  implicit val formatStreamTopic: RootJsonFormat[StreamTopic] = jsonFormat20(StreamTopic)
//  implicit val formatStreamSeqTopic: RootJsonFormat[StreamSeqTopic] = jsonFormat4(StreamSeqTopic)
//  implicit val formatStreamAdmin: RootJsonFormat[StreamAdmin] = jsonFormat6(StreamAdmin)
  implicit val formatRiderDatabase: RootJsonFormat[RiderDatabase] = jsonFormat3(RiderDatabase)
  implicit val formatRiderMonitor: RootJsonFormat[RiderMonitor] = jsonFormat4(RiderMonitor)
  implicit val formatRiderInfo: RootJsonFormat[RiderInfo] = jsonFormat9(RiderInfo)
  implicit val formatGrafanaConnectionInfo: RootJsonFormat[GrafanaConnectionInfo] = jsonFormat1(GrafanaConnectionInfo)
//  implicit val formatStreamResource: RootJsonFormat[StreamResource] = jsonFormat6(StreamResource)
//  implicit val formatResource: RootJsonFormat[Resource] = jsonFormat5(Resource)
//  implicit val formatStreamTopicActions: RootJsonFormat[StreamSeqTopicActions] = jsonFormat5(StreamSeqTopicActions)


  implicit val formatUdf: RootJsonFormat[Udf] = jsonFormat10(Udf)
  implicit val formatUdfProject: RootJsonFormat[UdfProject] = jsonFormat11(UdfProject)
  implicit val formatSimpleUdf: RootJsonFormat[SimpleUdf] = jsonFormat5(SimpleUdf)
  //app api
  implicit val formatSimpleJob: RootJsonFormat[AppJob] = jsonFormat15(AppJob)
  implicit val formatJobHealth: RootJsonFormat[JobHealth] = jsonFormat2(JobHealth)
  implicit val formatFlowHealth: RootJsonFormat[FlowHealth] = jsonFormat6(FlowHealth)
  implicit val formatTopicOffset: RootJsonFormat[TopicOffset] = jsonFormat2(TopicOffset)
  implicit val formatStreamHealth: RootJsonFormat[StreamHealth] = jsonFormat6(StreamHealth)
  implicit val formatAppFlow: RootJsonFormat[AppFlow] = jsonFormat11(AppFlow)
  implicit val formatAppFlowResponse: RootJsonFormat[AppFlowResponse] = jsonFormat2(AppFlowResponse)
  implicit val formatAppJobResponse: RootJsonFormat[AppJobResponse] = jsonFormat2(AppJobResponse)


  implicit object formatBaseEntity extends RootJsonFormat[BaseEntity] {

    def write(obj: BaseEntity): JsValue = obj match {
      case instance: Instance => instance.toJson
      case database: NsDatabase => database.toJson
      case namespace: Namespace => namespace.toJson
      case flow: Flow => flow.toJson
      case user: User => user.toJson
      case project: Project => project.toJson
      case udf: Udf => udf.toJson
      case unknown@_ => serializationError(s"Marshalling issue with $unknown.")
    }

    def read(value: JsValue): Nothing = {
      value.toString()
      value match {
        case unknown@_ => deserializationError(s"Unmarshalling issue with $unknown.")
      }
    }
  }

  implicit object formatSimpleBaseEntity extends RootJsonFormat[SimpleBaseEntity] {

    def write(obj: SimpleBaseEntity): JsValue = obj match {
      case instance: SimpleInstance => instance.toJson
      case database: SimpleNsDatabase => database.toJson
      case user: SimpleUser => user.toJson
      case project: SimpleProject => project.toJson
      case unknown@_ => serializationError(s"Marshalling issue with $unknown.")
    }

    def read(value: JsValue): Nothing = {
      value.toString()
      value match {
        case unknown@_ => deserializationError(s"Unmarshalling issue with $unknown.")
      }
    }
  }

}


