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

package edp.wormhole.flinkx.deserialization

import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsSchemaUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema
import org.apache.flink.types.Row
import edp.wormhole.flinkx.util.FlinkSchemaUtils.{umsType2FlinkType, s2TrueValue}

import scala.collection.mutable


class WormholeDeserializationSchema(schemaMap: mutable.HashMap[String, (Int, UmsFieldType)]) extends KeyedDeserializationSchema[Row] {
  val flinkTypeArray = schemaMap.values.map(_._2).toArray.map(umsType2FlinkType)
  val fieldIndexArray = schemaMap.values.map(_._1).toList
  val fieldNameArray = schemaMap.keySet.toArray

  println(fieldIndexArray)
  println(fieldNameArray)
  println(flinkTypeArray)

  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long) = {
    val deserializationSchema = new SimpleStringSchema()
    val key = if (messageKey != null) deserializationSchema.deserialize(messageKey) else null
    val value = if (message != null) deserializationSchema.deserialize(message) else null
    val ums = UmsSchemaUtils.toUms(value)


    ums.payload_get.map(_.tuple).map(tuple => {
      val row = new Row(tuple.size)
      for (i <- tuple.indices) row.setField(i, s2TrueValue(flinkTypeArray(i), tuple(fieldIndexArray(i))))
      row
    }).head
  }

  override def isEndOfStream(nextElement: Row): Boolean = false

  override def getProducedType: TypeInformation[Row] = {

    val types = new RowTypeInfo(flinkTypeArray, fieldNameArray)
    types
  }
}
