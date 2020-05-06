package edp.wormhole.sinks.kudusink

import edp.wormhole.kuduconnection.KuduConnection
import edp.wormhole.publicinterface.sinks.{SinkProcessConfig, SinkProcessor}
import edp.wormhole.sinks.SourceMutationType
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.ums.{UmsNamespace, UmsSysField}
import edp.wormhole.util.JsonUtils
import edp.wormhole.util.config.ConnectionConfig
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class Data2KuduSink extends SinkProcessor {
  private lazy val logger = Logger.getLogger(this.getClass)

  override def process(sourceNamespace: String,
                       sinkNamespace: String,
                       sinkProcessConfig: SinkProcessConfig,
                       schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)],
                       tupleList: Seq[Seq[String]],
                       connectionConfig: ConnectionConfig): Unit = {
    logger.info(s"sink kudu size is ${tupleList.size}")
    logger.info(s"sink kudu config is ${sinkProcessConfig}")
    KuduConnection.initKuduConfig(connectionConfig)

    val sinkSpecificConfig =
      if (sinkProcessConfig.specialConfig.isDefined) JsonUtils.json2caseClass[KuduConfig](sinkProcessConfig.specialConfig.get)
      else KuduConfig()
    val namespace = UmsNamespace(sinkNamespace)
    val tableName: String = namespace.table
    val database = namespace.database + sinkSpecificConfig.`table_connect_character.get`
    var allErrorsCount = 0

    val tableKeys: Seq[String] = sinkProcessConfig.tableKeyList
    tupleList.sliding(sinkSpecificConfig.`batch_size.get`, sinkSpecificConfig.`batch_size.get`).foreach(payload => {
      logger.info(s"size:${sinkSpecificConfig.`batch_size.get`},tuple size:${payload.length}")
      val tupleList: Seq[Seq[String]] = payload.toList
      try {
        if (sinkSpecificConfig.`mutation_type.get` == SourceMutationType.I_U_D.toString) {
          val keys2UmsIdMap: mutable.Map[String, Map[String, (Any, String)]] =
            if (tableKeys.length == 1)
              KuduConnection.doQueryByKeyListInBatch(tableName, database, connectionConfig.connectionUrl,
                tableKeys.head, tupleList, schemaMap, List(tableKeys.head, UmsSysField.ID.toString),
                sinkSpecificConfig.`query_batch_size.get`)
            else KuduConnection.doQueryByKeyList(tableName, database, connectionConfig.connectionUrl, tableKeys, tupleList, schemaMap, List {
              UmsSysField.ID.toString
            })
          val insertList = ListBuffer.empty[Seq[String]]
          val updateList = ListBuffer.empty[Seq[String]]
          if (keys2UmsIdMap == null || keys2UmsIdMap.isEmpty) insertList ++= tupleList
          else {
            tupleList.foreach(tuple => {
              val keysStr = tableKeys.map(keyName => {
                tuple(schemaMap(keyName)._1)
              }).mkString("_")
              if (keys2UmsIdMap.contains(keysStr)) {
                val umsidInKudu = keys2UmsIdMap(keysStr)(UmsSysField.ID.toString)._1.asInstanceOf[Long]
                val umsidInMem = tuple(schemaMap(UmsSysField.ID.toString)._1).toLong
                if (umsidInKudu < umsidInMem) updateList += tuple
              } else insertList += tuple
            })
          }

          if (insertList.nonEmpty) {
            val errorsCount = KuduConnection.doInsert(tableName, database, connectionConfig.connectionUrl, schemaMap, insertList)
            if (errorsCount > 0) {
              allErrorsCount = allErrorsCount + errorsCount
              logger.error("do sink error,count=" + errorsCount)
            }
          }
          if (updateList.nonEmpty) {
            val errorsCount = KuduConnection.doUpdate(tableName, database, connectionConfig.connectionUrl, schemaMap, updateList)
            if (errorsCount > 0) {
              allErrorsCount = allErrorsCount + errorsCount
              logger.error("do sink error,count=" + errorsCount)
            }
          }
        } else if (tupleList.nonEmpty) {
          val errorsCount = KuduConnection.doInsert(tableName, database, connectionConfig.connectionUrl, schemaMap, tupleList)
          if (errorsCount > 0) {
            allErrorsCount = allErrorsCount + errorsCount
            logger.error("do sink error,count=" + errorsCount)
          }
        }

      } catch {
        case e: Throwable =>
          allErrorsCount = allErrorsCount + tupleList.size
          logger.error("do sink error,count=" + tupleList.size, e)
      }
    })

    if (allErrorsCount > 0) throw new Exception("du kudu sink has error,count=" + allErrorsCount)

  }

}
