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


package edp.wormhole.ums

import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.UmsProtocolType.UmsProtocolType
import edp.wormhole.util.config.WormholeDefault._
import edp.wormhole.util.{CommonUtils, DateUtils}
import org.joda.time.DateTime

case class Ums(protocol: UmsProtocol,
               schema: UmsSchema,
               payload: Option[Seq[UmsTuple]] = None) {
  lazy val payload_get = payload.getOrElse(Nil)
}

case class UmsProtocol(`type`: UmsProtocolType,
                       version: Option[String] = Some("v1"),
                       msg_id: Option[Long] = Some(-1L),
                       msg_prev_id: Option[Long] = Some(-1L)) {
  lazy val version_get = version.getOrElse("v1")
  lazy val msg_id_get = msg_id.getOrElse(-1L)
  lazy val msg_prev_id_get = msg_prev_id.getOrElse(-1L)
}

case class UmsSchema(namespace: String,
                     fields: Option[Seq[UmsField]] = None) {
  lazy val umsNamespace = UmsNamespace(namespace)
  lazy val fields_get = fields.getOrElse(Nil)
}

case class UmsField(name: String,
                    `type`: UmsFieldType,
                    nullable: Option[Boolean] = Some(false)) {
  lazy val nullable_get = nullable.getOrElse(false)
}

case class UmsTuple(tuple: Seq[String])

object UmsFieldType extends Enumeration {
  type UmsFieldType = Value

  val STRING = Value("string")
  val INT = Value("int")
  val LONG = Value("long")
  val FLOAT = Value("float")
  val DOUBLE = Value("double")
  val BOOLEAN = Value("boolean")
  val DATE = Value("date")
  val DATETIME = Value("datetime")
  val DECIMAL = Value("decimal")
  val BINARY = Value("binary")
  val JSONARRAY = Value("jsonarray")
  val JSONOBJECT = Value("jsonobject")

  def classType = Value match {
    case STRING => classOf[String]
    case INT => classOf[Int]
    case LONG => classOf[Long]
    case FLOAT => classOf[Float]
    case DOUBLE => classOf[Double]
    case BOOLEAN => classOf[Boolean]
    case DATE => classOf[DateTime]
    case DATETIME => classOf[DateTime]
    case BINARY => classOf[Array[Byte]]
    case DECIMAL => classOf[java.math.BigDecimal]
  }

  def umsFieldType(s: String) = UmsFieldType.withName(s.toLowerCase)

  def umsFieldValue(v: String, umsFieldType: UmsFieldType): Any = umsFieldType match {
    case STRING => CommonUtils.any2string(nullify(v))
    case INT => CommonUtils.s2int(nullify(v))
    case LONG => CommonUtils.s2long(nullify(v))
    case FLOAT => CommonUtils.s2float(nullify(v))
    case DOUBLE => CommonUtils.s2double(nullify(v))
    case DECIMAL => CommonUtils.s2decimal(nullify(v))
    case BOOLEAN => CommonUtils.s2boolean(nullify(v))
    case BINARY => CommonUtils.base64s2byte(nullify(v))
    case DATE => DateUtils.dt2dateTime(nullify(v))
    case DATETIME => DateUtils.dt2dateTime(nullify(v))
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }

  def umsFieldValue(tuple: Seq[String], fields: Seq[UmsField], fieldName: String): Any = {
    val index = fields.map(_.name).indexOf(fieldName)
    if (index >= 0) umsFieldValue(tuple(index), fields(index).`type`) else null
  }
}

object UmsFeedbackStatus extends Enumeration {
  type UmsFeedbackStatus = Value

  val SUCCESS = Value("success")
  val FAIL = Value("fail")
}

object UmsSysField extends Enumeration {
  type UmsSysField = Value

  val UID = Value("ums_uid_")
  val ID = Value("ums_id_")
  val TS = Value("ums_ts_")
  val OP = Value("ums_op_")
  val ACTIVE = Value("ums_active_")//0:delete,1:ok

  def umsId(tuple: Seq[String], fields: Seq[UmsField]): Long =
    UmsFieldType.umsFieldValue(tuple, fields, ID.toString).asInstanceOf[Long]

  def umsTs(tuple: Seq[String], fields: Seq[UmsField]): DateTime =
    UmsFieldType.umsFieldValue(tuple, fields, TS.toString).asInstanceOf[DateTime]

  def umsOp(tuple: Seq[String], fields: Seq[UmsField]): UmsOpType.Value =
    UmsOpType.umsOpType(UmsFieldType.umsFieldValue(tuple, fields, OP.toString).asInstanceOf[String])
  def umsUid(tuple: Seq[String], fields: Seq[UmsField]): UmsOpType.Value =
    UmsOpType.umsOpType(UmsFieldType.umsFieldValue(tuple, fields, UID.toString).asInstanceOf[String])

  def umsSysField(s: String) = UmsSysField.withName(s.toLowerCase)
}

object UmsOpType extends Enumeration {
  type UmsOpType = Value

  val INSERT = Value("i")
  val UPDATE = Value("u")
  val BEFORE_UPDATE = Value("b")
  val DELETE = Value("d")

  def umsOpType(s: String) = UmsOpType.withName(s.toLowerCase)
}

object UmsActiveType {
  lazy val ACTIVE = 1
  lazy val INACTIVE = 0
}

object UmsProtocolType extends Enumeration {
  type UmsProtocolType = Value
  val DATA_INITIAL_DATA = Value("data_initial_data")
  val DATA_INCREMENT_HEARTBEAT = Value("data_increment_heartbeat")
  val DATA_INCREMENT_DATA = Value("data_increment_data")
  val DATA_INCREMENT_TERMINATION = Value("data_increment_termination")
  val DATA_BATCH_DATA = Value("data_batch_data")
  val DATA_BATCH_TERMINATION = Value("data_batch_termination")

  val DIRECTIVE_FLOW_START = Value("directive_flow_start")
  val DIRECTIVE_FLOW_STOP = Value("directive_flow_stop")
  val DIRECTIVE_TOPIC_SUBSCRIBE = Value("directive_topic_subscribe")
  val DIRECTIVE_TOPIC_UNSUBSCRIBE = Value("directive_topic_unsubscribe")
  val DIRECTIVE_HDFSLOG_FLOW_START = Value("directive_hdfslog_flow_start")
  val DIRECTIVE_HDFSLOG_FLOW_STOP = Value("directive_hdfslog_flow_stop")
  val DIRECTIVE_HDFSCSV_FLOW_START = Value("directive_hdfscsv_flow_start")
  val DIRECTIVE_HDFSCSV_FLOW_STOP = Value("directive_hdfscsv_flow_stop")
  val DIRECTIVE_ROUTER_FLOW_START = Value("directive_router_flow_start")
  val DIRECTIVE_ROUTER_FLOW_STOP = Value("directive_router_flow_stop")
  val DIRECTIVE_UDF_ADD = Value("directive_udf_add")

  val FEEDBACK_DATA_BATCH_TERMINATION = Value("feedback_data_batch_termination")
  val FEEDBACK_DATA_INCREMENT_HEARTBEAT = Value("feedback_data_increment_heartbeat")
  val FEEDBACK_DATA_INCREMENT_TERMINATION = Value("feedback_data_increment_termination")
  val FEEDBACK_FLOW_START_DIRECTIVE = Value("feedback_flow_start_directive")
  val FEEDBACK_FLOW_ERROR = Value("feedback_flow_error")
  val FEEDBACK_FLOW_STATS = Value("feedback_flow_stats")

  @Deprecated
  val FEEDBACK_SPARKX_FLOW_STATS = Value("feedback_sparkx_flow_stats")
  @Deprecated
  val FEEDBACK_DIRECTIVE = Value("feedback_directive")
  @Deprecated
  val FEEDBACK_SPARKX_FLOW_ERROR = Value("feedback_sparkx_flow_error")
  @Deprecated
  val FEEDBACK_FLOW_SPARKX_ERROR = Value("feedback_flow_sparkx_error")
  @Deprecated
  val FEEDBACK_FLINKX_FLOW_ERROR = Value("feedback_flinkx_flow_error")
  @Deprecated
  val FEEDBACK_STREAM_BATCH_ERROR = Value("feedback_stream_batch_error")
  @Deprecated
  val FEEDBACK_STREAM_TOPIC_OFFSET = Value("feedback_stream_topic_offset")


  def umsProtocolType(s: String) = UmsProtocolType.withName(s.toLowerCase)


}

object DataTypeEnum extends Enumeration {
  type DataTypeEnum = Value

  val UMS = Value("ums")
  val UMS_EXTENSION = Value("ums_extension")
}


