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

package edp.wormhole.flinkx.eventflow

import edp.wormhole.common.json.{FieldInfo, JsonParseUtils}
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.ums.{UmsCommonUtils, UmsField}
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.apache.flink.util.Collector
import org.apache.log4j.Logger

import scala.collection.mutable.ArrayBuffer

class UmsFlatMapper(sourceSchemaMap: Map[String, (TypeInformation[_], Int)], sourceNamespace: String, jsonSourceParseMap: Map[(UmsProtocolType, String), (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)])]) extends RichFlatMapFunction[(String, String, String, Int, Long), Row] with Serializable {
  private lazy val logger = Logger.getLogger(this.getClass)

  override def flatMap(value: (String, String, String, Int, Long), out: Collector[Row]): Unit = {
    logger.info("in UmsFlatMapper source data from kafka " + value._2)
    val (protocolType, namespace) = UmsCommonUtils.getTypeNamespaceFromKafkaKey(value._1)
    if (jsonSourceParseMap.contains((protocolType, namespace))) {
      val mapValue: (Seq[UmsField], Seq[FieldInfo], ArrayBuffer[(String, String)]) = jsonSourceParseMap((protocolType, namespace))
      val umsTuple = JsonParseUtils.dataParse(value._2, mapValue._2, mapValue._3)
      umsTuple.foreach(tuple => {
        createRow(tuple.tuple, protocolType.toString, out)
      })
    }
    else {
        val ums = UmsCommonUtils.json2Ums(value._2)
        logger.info("in UmsFlatMapper " + sourceSchemaMap.size)
        if (FlinkSchemaUtils.matchNamespace(ums.schema.namespace, sourceNamespace) && ums.payload.nonEmpty && ums.schema.fields.nonEmpty)
          ums.payload_get.foreach(tuple => {
            createRow(tuple.tuple, protocolType.toString, out)
          })
    }
  }
  def createRow(tuple: Seq[String], protocolType:String, out: Collector[Row]): Unit = {
    val row = new Row(tuple.size + 1)
    row.setField(0, protocolType)
    for (i <- 1 to tuple.size)
      row.setField(i, FlinkSchemaUtils.getRelValue(i, tuple(i - 1), sourceSchemaMap))
    out.collect(row)
  }
}
