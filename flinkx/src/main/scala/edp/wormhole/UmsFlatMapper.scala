/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2018 EDP
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

package edp.wormhole

import edp.wormhole.ums.UmsCommonUtils
import edp.wormhole.util.FlinkSchemaUtils
import edp.wormhole.util.FlinkSchemaUtils.matchNamespace
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

class UmsFlatMapper(sourceSchemaMap: Map[String, (TypeInformation[_], Int)], sourceNamespace: String) extends RichFlatMapFunction[(String, String, String, Int, Long), Row] with Serializable {
  private lazy val logger = Logger.getLogger(this.getClass)

  override def flatMap(value: (String, String, String, Int, Long), out: Collector[Row]): Unit = {
    logger.info("in UmsFlatMapper source data from kafka " + value._2)
    val ums = UmsCommonUtils.json2Ums(value._2)
    logger.info("in UmsFlatMapper " + sourceSchemaMap.size)
    sourceSchemaMap.foreach(println)
    if (matchNamespace(ums.schema.namespace, sourceNamespace) && ums.payload.nonEmpty && ums.schema.fields.nonEmpty)
      ums.payload_get.map(_.tuple).foreach(tuple => {
        val row = new Row(tuple.size + 1)
        row.setField(0, ums.protocol.`type`.toString)
        for (i <- 1 to tuple.size)
          row.setField(i, FlinkSchemaUtils.getRelValue(i, tuple(i - 1), sourceSchemaMap))
        out.collect(row)
      })
  }
}
