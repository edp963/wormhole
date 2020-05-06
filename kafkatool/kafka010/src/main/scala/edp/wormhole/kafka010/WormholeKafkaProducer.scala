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


package edp.wormhole.kafka010

import java.util.Properties

import edp.wormhole.util.config.KVConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger

import scala.collection.mutable

object WormholeKafkaProducer extends Serializable {

  @volatile private var producerMap: mutable.HashMap[String, KafkaProducer[String, String]] = new mutable.HashMap[String, KafkaProducer[String, String]]
  @transient lazy val logger = Logger.getLogger(this.getClass)

  private def getProducerProps: Properties = {
    val props = new Properties()
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("acks", "all")
    props.put("compression.type", "lz4")
    props.put("max.request.size", 10485760.toString)
    props
  }

  def init(brokers: String, kvConfig: Option[Seq[KVConfig]],kerberos:Boolean=false): Unit = {

    if (!producerMap.contains(brokers) || producerMap(brokers) == null) {
      synchronized {
        if (!producerMap.contains(brokers) || producerMap(brokers) == null) {
          val props = getProducerProps
          if (kvConfig.nonEmpty) {
            kvConfig.get.foreach(kv => {
              props.put(kv.key, kv.value)
            })
          }

          if(kerberos){
            props.put("security.protocol","SASL_PLAINTEXT")
            props.put("sasl.kerberos.service.name","kafka")
          }
          props.put("bootstrap.servers", brokers)
          producerMap(brokers) = new KafkaProducer[String, String](props)
        }
      }
    }
  }

  def sendMessage(topic: String, message: String, key: Option[String], brokers: String): Any = send(topic, message, key, brokers)

  def sendMessage(topic: String, partition: Int = 0, message: String, key: Option[String], brokers: String): Any = send(topic, partition, message, key, brokers)

  private def send(topic: String, message: String, key: Option[String], brokers: String): Any = {
    try {
     // println("producerMap"+producerMap.toString()+",brokers:"+brokers)
      sendInternal(topic, message, key, brokers)
    } catch {
      case _: Throwable =>
        try {
          sendInternal(topic, message, key, brokers)
        } catch {
          case re: Throwable => throw re
        }
    }
  }

  private def send(topic: String, partition: Int, message: String, key: Option[String], brokers: String): Any = {
    try {
      sendInternal(topic, partition, message, key, brokers)
    } catch {
      case _: Throwable =>
        try {
          sendInternal(topic, partition, message, key, brokers)
        } catch {
          case re: Throwable => throw re
        }
    }
  }

  def close(brokers: String): Unit =
    try {
      if (producerMap(brokers) != null)
        producerMap(brokers).close()
      producerMap -= brokers
    } catch {
      case e: Throwable => logger.error("close - ERROR", e)
    }

  private def getProducer(brokers: String): KafkaProducer[String, String] = {
    val kafkaProducer = producerMap(brokers)
    if(null == kafkaProducer) {
      logger.error(s"get kafkaProducer failed, producerMap not contain $brokers")
    }
    kafkaProducer
  }

  private def sendInternal(topic: String, message: String, key: Option[String], brokers: String) =
    if (message != null) {
      try {
        if (key.isDefined) {
          //logger.info("kafka send message with key")
          val future = getProducer(brokers).send(new ProducerRecord[String, String](topic, key.get, message))
          future.get()
        } else {
          //logger.info("kafka send message without key")
          val future = getProducer(brokers).send(new ProducerRecord[String, String](topic, message))
          future.get()
        }
      } catch {
        case e: Throwable =>
          logger.error("sendInternal - send ERROR:", e)
          try {
            close(brokers)
          } catch {
            case closeError: Throwable =>
              logger.error("sendInternal - close ERROR,", closeError)
              producerMap -= brokers
          }
          //producerMap = null
          throw e
      }
    }

  private def sendInternal(topic: String, partition: Int, message: String, key: Option[String], brokers: String) =
    if (message != null) {
      try {
        if (key.isDefined) {
          val future = getProducer(brokers).send(new ProducerRecord[String, String](topic, partition, key.get, message))
          future.get()
        } else {
          val future = getProducer(brokers).send(new ProducerRecord[String, String](topic, partition, null, message))
          future.get()
        }
      } catch {
        case e: Throwable =>
          logger.error("sendInternal - send ERROR:", e)
          try {
            close(brokers)
          } catch {
            case closeError: Throwable =>
              logger.error("sendInternal - close ERROR,", closeError)
              producerMap -= brokers
          }
          //producerMap = null
          throw e
      }
    }
}
