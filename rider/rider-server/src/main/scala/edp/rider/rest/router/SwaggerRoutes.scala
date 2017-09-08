///*-
// * <<
// * wormhole
// * ==
// * Copyright (C) 2016 - 2017 EDP
// * ==
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// * >>
// */
//
//
//package edp.rider.rest.router
//
//import akka.actor.ActorSystem
//import akka.stream.ActorMaterializer
//import com.github.swagger.akka.model.Info
//import com.github.swagger.akka.{HasActorSystem, SwaggerHttpService}
//import edp.rider.RiderStarter
//import edp.rider.common.RiderConfig
//import edp.rider.rest.router.admin.routes._
//import edp.rider.rest.router.app.routes.{FlowAppRoutes, JobAppRoutes}
//import edp.rider.rest.router.user.routes._
//
//import scala.reflect.runtime.universe._
//
//class SwaggerRoutes extends SwaggerHttpService with HasActorSystem {
//  override implicit val actorSystem: ActorSystem = RiderStarter.system
//  override implicit val materializer: ActorMaterializer = RiderStarter.materializer
//  override val apiTypes = Seq(
//    typeOf[LoginRoutes],
//    typeOf[ChangePwdRoutes],
//    typeOf[InstanceAdminRoutes],
//    typeOf[NsDatabaseAdminRoutes],
//    typeOf[NamespaceAdminRoutes],
//    typeOf[StreamAdminRoutes],
//    typeOf[FlowAdminRoutes],
//    typeOf[UserAdminRoutes],
//    typeOf[ProjectAdminRoutes],
//    typeOf[ProjectUserRoutes],
//    typeOf[NamespaceUserRoutes],
//    typeOf[StreamUserRoutes],
//    typeOf[FlowUserRoutes],
//    typeOf[ActionUserRoutes],
//    typeOf[RiderInfoAdminRoutes],
//    typeOf[NsDatabaseUserRoutes],
//    typeOf[FlowAppRoutes],
//    typeOf[JobAppRoutes]
//  )
//
//  val domain = RiderStarter.modules.config.getString("riderServer.http.domain.name")
//  override val host = if(domain == null || domain == "") RiderStarter.host + ":" + RiderStarter.port else domain
//  //the url of your api, not swagger's json endpoint
//  override val basePath = "/api/v1"
//  //the basePath for the API you are exposing
//  override val apiDocsPath = "api-docs"
//  //where you want the swagger-json endpoint exposed
//  override val info = Info("Wh-Rider REST API")
//  //  provides license and other description details
//  val indexRoute = get {
//    pathPrefix("swagger") {
//      pathEndOrSingleSlash {
//        getFromFile(s"${RiderConfigInfo.riderConfPath}/swagger-ui/index.html")
//      }
//    } ~ getFromDirectory(s"${RiderConfigInfo.riderConfPath}/swagger-ui")
//  }
//}
