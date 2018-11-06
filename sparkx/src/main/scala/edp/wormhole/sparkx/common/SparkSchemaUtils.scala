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


package edp.wormhole.sparkx.common

import edp.wormhole.ums.UmsFieldType._
import edp.wormhole.ums._
import edp.wormhole.util.CommonUtils
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.config.WormholeDefault
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSchemaUtils extends SparkSchemaUtils

trait SparkSchemaUtils {
  private val TypeMap = Seq(
    (STRING, StringType),
    (INT, IntegerType),
    (LONG, LongType),
    (FLOAT, FloatType),
    (DOUBLE, DoubleType),
    (BOOLEAN, BooleanType),
    (DATE, DateType),
    (DATETIME, TimestampType),
    (DECIMAL, DecimalType.SYSTEM_DEFAULT),
    (BINARY, BinaryType)
  )

  def ums2sparkType(umsType: UmsFieldType): DataType = {
    TypeMap.find(_._1 == umsType) match {
      case Some(pair) => pair._2
      case None => throw new UnsupportedOperationException(s"Unknown UMS type: $umsType")
    }
  }

  def spark2umsType(dataType: DataType): UmsFieldType = {
    TypeMap.find(_._2 == dataType) match {
      case Some(pair) => pair._1
      case None => throw new UnsupportedOperationException(s"Unknown Data type: $dataType")
    }
  }


  def toTypedValue(value: String, dataType: DataType):Any = {
    if (value == null) null
    else dataType match {
      case StringType => value
      case IntegerType => value.toInt
      case LongType => value.toLong
      case FloatType => value.toFloat
      case DoubleType => value.toDouble
      case BooleanType => value.toBoolean
      case DateType => dt2sqlDate(value)
      case BinaryType => CommonUtils.base64s2byte(value)
      case DecimalType() => new java.math.BigDecimal(value)
      case TimestampType => new java.sql.Timestamp(dt2date(value).getTime)
      case _ => throw new UnsupportedOperationException(s"Unknown DataType: $dataType")
    }
  }

  def s2sparkValue(value: String, umsFieldType: UmsFieldType):Any = {
    toTypedValue(value, ums2sparkType(umsFieldType))
  }

  def ums2sparkSchema(schema: Seq[UmsField]): StructType =
    StructType(schema.map(f => StructField(f.name, ums2sparkType(f.`type`), f.nullable_get, Metadata.empty)))

  def ss2sparkTuple(schema: Seq[UmsField], tuple: Seq[String]): Row = new GenericRowWithSchema(
    (for (i <- schema.indices) yield s2sparkValue(tuple(i), schema(i).`type`)).toArray, ums2sparkSchema(schema))

  def createDf(spark: SparkSession, schema: Seq[UmsField], payload: RDD[Row]):DataFrame = {
    spark.createDataFrame(payload, ums2sparkSchema(schema))
  }



//  def df2umsAndSend(namespace: String, df: DataFrame, topicName: String, brokers: String, batchSize: Int):Unit = {
//    val protocol = UmsProtocol(UmsProtocolType.SWIFTS_INCREMENT_DATA)
//
//    val fields = df.schema.map { field =>
//      UmsField(field.name, spark2umsType(field.dataType), Some(field.nullable))
//    }
//
//    val schema = UmsSchema(namespace, Some(fields))
//
//    df.foreachPartition(mapRows => {
//      WormholeKafkaProducer.init(brokers)
//      val payloads: Iterator[UmsTuple] = mapRows.map(row => {
//        UmsTuple(fields.map(field => {
//          if (null == field) null
//          else {
//            val fieldContent = row.get(row.fieldIndex(field.name))
//            if (null == fieldContent) null else fieldContent.toString
//          }
//        }))
//      })
//
//      if (batchSize > 1) {
//        val payloadsSliding = payloads.sliding(batchSize, batchSize)
//        payloadsSliding.foreach(t => {
//          val ums = Ums(protocol, schema, Some(t))
//          WormholeKafkaProducer.sendMessage(topicName, UmsSchemaUtils.toJsonCompact(ums), None)
//        })
//      } else {
//        payloads.foreach(t => {
//          val ums = Ums(protocol, schema, Some(List(t)))
//          println("join failed ums:" + ums.toString)
//          WormholeKafkaProducer.sendMessage(topicName, UmsSchemaUtils.toJsonCompact(ums), None)
//        })
//      }
//    })
//  }
  /* ===================== ums schema/type system mapping ===================== */
  def spark2umsSchema(sparkType: StructType, namespace: String = WormholeDefault.empty): UmsSchema =
    UmsSchema(namespace, Some(sparkType.map(st => UmsField(st.name, spark2umsType(st.dataType), Some(st.nullable)))))


  def ums2sparkSchema(umsSchema: UmsSchema): StructType = ums2sparkSchema(umsSchema.fields_get)


  /* spark value convert to ums value */
  def spark2umsValue(umsFieldType: UmsFieldType, value: Any): Any = if (value == null) null
  else umsFieldType match {
    case UmsFieldType.STRING => value.asInstanceOf[String]
    case UmsFieldType.INT => value.asInstanceOf[Int]
    case UmsFieldType.LONG => value match {
      case _:Int => value.asInstanceOf[Int].toLong
      case _ => value.asInstanceOf[Long]
    }
    case UmsFieldType.FLOAT => value.asInstanceOf[Float]
    case UmsFieldType.DOUBLE => value match {
      case _: Float => value.asInstanceOf[Float].toDouble
      case _ => value.asInstanceOf[Double]
    }
    case UmsFieldType.BOOLEAN => value.asInstanceOf[Boolean]
    case UmsFieldType.DATE => dt2dateTime(value.asInstanceOf[java.sql.Date])
    case UmsFieldType.DATETIME => dt2dateTime(value.asInstanceOf[java.sql.Timestamp])
    case UmsFieldType.DECIMAL => value.asInstanceOf[WormholeDefault.JavaBigDecimal].stripTrailingZeros()
    case UmsFieldType.BINARY => value.asInstanceOf[WormholeDefault.JavaByteArray]
    case _ => throw new UnsupportedOperationException(s"Unknown Type: $umsFieldType")
  }


}

