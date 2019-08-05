package edp.wormhole.sinks.clickhousesink

import edp.wormhole.sinks.utils.SinkCommonUtils
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.config.ConnectionConfig
import net.openhft.hashing.LongHashFunction
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ClickhouseUtils {
  private lazy val logger = Logger.getLogger(this.getClass)
  def getTuplesShardMap(tupleList: Seq[Seq[String]], connectionConfig: ConnectionConfig, keyList: List[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): Map[ConnectionConfig, Seq[Seq[String]]] = {
    val shardServers = connectionConfig.connectionUrl.split(",")
    val hashFunction = LongHashFunction.xx()
    val shardServersSorted = shardServers.sortWith((left, right) => {
      logger.info(s"sort left ${hashFunction.hashChars(getIpPort(left))}, right ${hashFunction.hashChars(getIpPort(right))}")
      hashFunction.hashChars(getIpPort(left)) < hashFunction.hashChars(getIpPort(right))
    }) //取ip:port从小到大排序

    val sharpTupleMap = mutable.HashMap.empty[ConnectionConfig, mutable.ListBuffer[Seq[String]]]
    tupleList.foreach(tuple => {
      val shardValue = SinkCommonUtils.keyList2values(keyList, schemaMap, tuple)
      val curShardUrl = selectShardUrl(shardValue, shardServersSorted)
      val shardConnection = ConnectionConfig(curShardUrl, connectionConfig.username, connectionConfig.password, connectionConfig.parameters)
      val curTuples = sharpTupleMap.getOrElse(shardConnection, ListBuffer.empty[Seq[String]])
      curTuples.append(tuple)
      sharpTupleMap.put(shardConnection, curTuples)
    })
    sharpTupleMap.toMap
  }

  def selectShardUrl(shardValue: String, shardServersSorted: Seq[String]): String = {
    val hashFunction = LongHashFunction.xx()
    val hashNum = math.abs(hashFunction.hashChars(shardValue))
    logger.info(s"shardvalue: $shardValue; shardServersSorted: $shardServersSorted; hashNum: $hashNum;")
    val curShardUrl = shardServersSorted((hashNum % shardServersSorted.size).toInt)
    logger.info(s"curShardUrl: $curShardUrl")
    curShardUrl
  }
  def getIpPort(jdbcUrl: String): String = {
    val ipPort = jdbcUrl.split("//")(1).split("/")(0)
    logger.info(s"jdbcUrl $jdbcUrl, ipPort $ipPort")
    ipPort
  }

}
