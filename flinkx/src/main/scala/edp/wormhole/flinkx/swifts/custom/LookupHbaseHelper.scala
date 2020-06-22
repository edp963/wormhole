package edp.wormhole.flinkx.swifts.custom


import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.types.Row
import org.apache.log4j.Logger

import scala.collection.{mutable,Map}
import scala.collection.mutable.ListBuffer
import edp.wormhole.ums._
import edp.wormhole.util.config.ConnectionConfig
import edp.wormhole.util.swifts.SwiftsSql
import edp.wormhole.flinkx.util.FlinkSchemaUtils
import edp.wormhole.flinkx.util.FlinkSchemaUtils._
import edp.wormhole.hbaseconnection.{HbaseConnection, RowkeyPatternContent, RowkeyPatternType, RowkeyTool}
import edp.wormhole.swifts.ConnectionMemoryStorage

object LookupHbaseHelper extends java.io.Serializable{
  private lazy val logger = Logger.getLogger(this.getClass)
  def covertResultSet2Map(swiftsSql: SwiftsSql,
                          row: Row,
                          preSchemaMap: Map[String, (TypeInformation[_], Int)],
                          dbOutPutSchemaMap: Map[String, (String, String, Int)],
                          sourceTableFields: Array[String],
                          dataStoreConnectionsMap: Map[String, ConnectionConfig]): mutable.HashMap[String, ListBuffer[Array[Any]]] = {

    val dataTupleMap = mutable.HashMap.empty[String, mutable.ListBuffer[Array[Any]]]
    val (joinFieldsAsKey,dbOutputArray)=getDataFromHbase(row,preSchemaMap,dbOutPutSchemaMap,swiftsSql,sourceTableFields,dataStoreConnectionsMap)
    if (!dataTupleMap.contains(joinFieldsAsKey)) {
      dataTupleMap(joinFieldsAsKey) = ListBuffer.empty[Array[Any]]
    }
    dataTupleMap(joinFieldsAsKey) += dbOutputArray
    dataTupleMap
  }

  def getRowDatas(row:Row,preSchemaMap: Map[String, (TypeInformation[_], Int)]):Seq[String]={
    preSchemaMap.map(fieldDesc=>{
       val value=row.getField(fieldDesc._2._2)
      if(value!=null) value.toString else "N/A"
    }).toSeq
  }

  def getDataFromHbase(row:Row,preSchemaMap: Map[String, (TypeInformation[_], Int)],dbOutPutSchemaMap: Map[String, (String, String, Int)],swiftsSql: SwiftsSql,sourceTableFields: Array[String],dataStoreConnectionsMap: Map[String, ConnectionConfig]):(String,Array[Any]) ={
    val (tablename,cf,key,selectFields,connectionConfig)=resolutionOfSwiftSql(row,preSchemaMap,dbOutPutSchemaMap,swiftsSql,dataStoreConnectionsMap,sourceTableFields)

    HbaseConnection.initHbaseConfig(null,connectionConfig)
    val (ips, port, _) = HbaseConnection.getZookeeperInfo(connectionConfig.connectionUrl)
    val hbaseData=HbaseConnection.getDatasFromHbase(tablename,cf,true,Seq(key),selectFields.map(f=>(f._1,f._2)),ips,port)

    buildOutputTuple(row,preSchemaMap,hbaseData,key,sourceTableFields,selectFields)
  }

  def resolutionOfSwiftSql(row:Row,preSchemaMap: Map[String, (TypeInformation[_], Int)],dbOutPutSchemaMap: Map[String, (String, String, Int)],swiftsSql: SwiftsSql,dataStoreConnectionsMap: Map[String, ConnectionConfig],sourceTableFields:Array[String]):(String,String,String,Array[(String, String,String)],ConnectionConfig)={
    val lookupNamespace: String = if (swiftsSql.lookupNamespace.isDefined) swiftsSql.lookupNamespace.get else null
    val connectionConfig: ConnectionConfig = ConnectionMemoryStorage.getDataStoreConnectionsWithMap(dataStoreConnectionsMap, lookupNamespace)
  //  val patternContentList: mutable.Seq[RowkeyPatternContent] = RowkeyTool.parse(swiftsSql.sourceTableFields.get(0))
    val key=createJoinFieldAsKey(row,preSchemaMap,sourceTableFields)

    val selectFields=dbOutPutSchemaMap.keys.map(key=>(key,UmsFieldType.umsFieldType(dbOutPutSchemaMap(key)._2).toString,key)).toArray
    val fromIndex = swiftsSql.sql.indexOf(" from ")
    val table2cfGrp = swiftsSql.sql.substring(fromIndex + 6, swiftsSql.sql.indexOf(")", fromIndex)).split("\\(")
    logger.info("table2cfGrp:" + table2cfGrp(0) + "," + table2cfGrp(1))

    (table2cfGrp(0),table2cfGrp(1),key.head,selectFields,connectionConfig)
  }

  def buildOutputTuple(row:Row,preSchemaMap: Map[String, (TypeInformation[_], Int)],hbaseData:Map[String, Map[String, Any]],key:String,sourceTableFields:Array[String],selectFields: Array[(String, String,String)]):(String,Array[Any])={
    val tmpMap = mutable.HashMap.empty[String, Any]
    val dbOutputArray: Array[Any] = selectFields.map { case (name, dataType, newName) =>{
      val outputValue=if (hbaseData==null || hbaseData.isEmpty) s2TrueValue(s2FlinkType(dataType),null)
      else s2TrueValue(s2FlinkType(dataType),hbaseData(key)(name).toString)
      if(newName!=null && !newName.trim.equals(""))tmpMap(newName)=outputValue
      else tmpMap(name)=outputValue
      outputValue
    }}

    val joinFieldsAsKey = createJoinFieldAsKey(row,preSchemaMap,sourceTableFields).mkString("_")
    (joinFieldsAsKey,dbOutputArray)
  }

  def createJoinFieldAsKey(row:Row,preSchemaMap: Map[String, (TypeInformation[_], Int)],sourceTableFields:Array[String])={
    sourceTableFields.flatMap(fieldName => {
      val patternContentList = RowkeyTool.parse(fieldName)
      val fieldValueArray=patternContentList.map(field =>
        field.patternType.toString match {
          case "expression"=>{
            val value = FlinkSchemaUtils.object2TrueValue(preSchemaMap(field.fieldContent.trim)._1, row.getField(preSchemaMap(field.fieldContent.trim)._2))
            value.toString
          }
          case _=>field.fieldContent.trim
        })
      if (fieldValueArray != null) Seq(RowkeyTool.generatePatternKey(fieldValueArray, patternContentList)) else Seq("N/A")
    })
  }

  def joinFieldsInRow(row: Row,
                      lookupTableFields: Array[String],
                      sourceTableFields: Array[String],
                      preSchemaMap: Map[String, (TypeInformation[_], Int)]): Array[String] = {
    val fieldContent = createJoinFieldAsKey(row,preSchemaMap,sourceTableFields)
    if (!fieldContent.contains("N/A")) {
      fieldContent
    } else Array.empty[String]
  }
}
