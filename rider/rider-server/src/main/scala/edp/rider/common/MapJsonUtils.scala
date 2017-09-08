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


package edp.rider.common

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.json4s.jackson.Serialization

object MapJsonUtils extends RiderLogger{
  def map2Json(map: Map[String, Any]): String = {
    implicit val formats = org.json4s.DefaultFormats
    Serialization.write(map)
  }

  def json2Map(jsonStr: String): Map[String, Any] = {
    try {
      val mapper = new ObjectMapper() with ScalaObjectMapper
      mapper.registerModule(DefaultScalaModule)
      mapper.readValue[Map[String, Any]](jsonStr)
    } catch {
      case t: Throwable =>
        riderLogger.error(s"json string $jsonStr convert to map failed", t)
        scala.collection.immutable.Map[String, Any]()
    }
  }

  def object2json[T <: AnyRef](obj: T): String = {
    implicit val formats = org.json4s.DefaultFormats
    Serialization.write[T](obj)
  }
}
