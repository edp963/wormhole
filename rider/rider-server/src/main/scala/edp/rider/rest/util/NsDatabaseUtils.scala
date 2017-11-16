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


package edp.rider.rest.util

import com.alibaba.fastjson.JSON
import edp.rider.rest.util.CommonUtils.{isJson, isKeyEqualValue}
import edp.wormhole.common.KVConfig

import scala.collection.mutable.ListBuffer


object NsDatabaseUtils {

  def getDbConfig(nsSys: String, config: String): Option[Seq[KVConfig]] = {
    nsSys match {
      case "mysql" | "postgresql" | "phoenix" => None
      case "oracle" => None
      case _ =>
        if (config == null || config == "") None
        else if (isJson(config)) {
          val seq = new ListBuffer[KVConfig]
          val json = JSON.parseObject(config)
          val keySet = json.keySet().toArray
          keySet.foreach(key => seq += KVConfig(key.toString, json.get(key).toString))
          Some(seq)
        } else if (isKeyEqualValue(config)) {
          val seq = new ListBuffer[KVConfig]
          val keyValueSeq = config.split(",").mkString("&").split("&")
          keyValueSeq.foreach(
            keyValue => {
              val data = keyValue.split("=")
              seq += KVConfig(data(0), data(1))
            }
          )
          Some(seq)
        } else None
    }
  }

}


