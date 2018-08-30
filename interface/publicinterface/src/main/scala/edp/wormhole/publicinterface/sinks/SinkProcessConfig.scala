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


package edp.wormhole.publicinterface.sinks

case class SinkProcessConfig(sinkOutput:String,
                             tableKeys: Option[String],
                             specialConfig: Option[String],
                             jsonSchema:Option[String],
                             classFullname: String,
                             retryTimes: Int,
                             retrySeconds: Int){
  lazy val tableKeyList = if (tableKeys.isEmpty || tableKeys.get == null) Nil else tableKeys.get.split(",").map(_.trim.toLowerCase).toList
}

