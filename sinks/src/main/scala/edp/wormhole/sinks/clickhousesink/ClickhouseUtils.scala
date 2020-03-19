package edp.wormhole.sinks.clickhousesink

import java.nio.ByteBuffer

import edp.wormhole.sinks.utils.SinkCommonUtils
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.config.ConnectionConfig
import net.jpountz.xxhash.{XXHash64, XXHashFactory}
import org.apache.log4j.Logger

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ClickhouseUtils {
  private lazy val logger = Logger.getLogger(this.getClass)
  def getTuplesShardMap(tupleList: Seq[Seq[String]], connectionConfig: ConnectionConfig, keyList: List[String], schemaMap: collection.Map[String, (Int, UmsFieldType, Boolean)]): Map[ConnectionConfig, Seq[Seq[String]]] = {
    val shardServers = connectionConfig.connectionUrl.split(",")
    /*val hashFunction = LongHashFunction.xx()
    val shardServersSorted = shardServers.sortWith((left, right) => {
      hashFunction.hashChars(getIpPort(left)) < hashFunction.hashChars(getIpPort(right))
    }) //取ip:port从小到大排序*/
    val sharpTupleMap = mutable.HashMap.empty[ConnectionConfig, mutable.ListBuffer[Seq[String]]]
    tupleList.foreach(tuple => {
      val shardKey = SinkCommonUtils.keyList2values(keyList, schemaMap, tuple)
      val curShardUrl = shardServers(selectShardNum(shardKey, shardServers.size))
      val shardConnection = ConnectionConfig(curShardUrl, connectionConfig.username, connectionConfig.password, connectionConfig.parameters)
      val curTuples = sharpTupleMap.getOrElse(shardConnection, ListBuffer.empty[Seq[String]])
      curTuples.append(tuple)
      sharpTupleMap.put(shardConnection, curTuples)
    })
    sharpTupleMap.toMap
  }

  def selectShardNum(shardKey: String, shardServersSize: Int): Int = {
    val hasher = XXHashFactory.fastestInstance.hash64
    val longHashNum = hasher.hash(ByteBuffer.wrap(shardKey.getBytes()), 0)
    val uInt64HashNum = long2UInt64(longHashNum)
    val shardNum = uInt64HashNum % shardServersSize
//    logger.info(s"shardKey: $shardKey, longHashNum: $longHashNum, uInt64HashNum $uInt64HashNum, shardNum: $shardNum")
    shardNum.toInt
  }

  def long2UInt64(longValue: Long): BigDecimal = {
    if(longValue >= 0) {
      BigDecimal(longValue)
    } else {
      val lowValue = longValue & 0x7fffffffffffffffL
      BigDecimal(lowValue) + BigDecimal.valueOf(Long.MaxValue) + BigDecimal.valueOf(1)
    }
  }

/*  def getIpPort(jdbcUrl: String): String = {
    jdbcUrl.split("//")(1).split("/")(0)
  }*/
}
