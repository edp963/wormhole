package edp.wormhole.dbdriver.rocketmq

import edp.wormhole.util.config.KVConfig

case class MqMessage(topic: String,
                     body: String,
                     tags: Option[String] = None,
                     keys: Option[String] = None,
                     flag: Option[Int] = None,
                     waitStoreMsgOK: Option[Boolean] = None,
                     kvConfig: Option[Seq[KVConfig]] = None)
