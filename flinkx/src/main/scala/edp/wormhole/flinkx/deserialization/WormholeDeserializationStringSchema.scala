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

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema

class WormholeDeserializationStringSchema extends KeyedDeserializationSchema[(String,String,String,Int,Long)]{
  //var keyValueTopic:KeyValueTopic = _
  override def deserialize(messageKey: Array[Byte], message: Array[Byte], topic: String, partition: Int, offset: Long): (String,String,String,Int,Long) = {
    val deserializationSchema = new SimpleStringSchema()
    val key = if (messageKey != null) deserializationSchema.deserialize(messageKey) else null
    val value = if (message != null) deserializationSchema.deserialize(message) else null
    (key,value,topic,partition,offset)
  }

  override def isEndOfStream(nextElement: (String,String,String,Int,Long)): Boolean = false

  override def getProducedType: TypeInformation[(String,String,String,Int,Long)] = {
    TypeInformation.of(new TypeHint[(String,String,String,Int,Long)] {})
  }
}
