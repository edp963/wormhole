package edp.wormhole.sparkx.swifts.custom.bdp

import com.alibaba.fastjson.JSON
import edp.wormhole.sparkx.common.SparkSchemaUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{WormholeConfig, SwiftsProcessConfig, SwiftsInterface}
import edp.wormhole.ums.{UmsFieldType, UmsSchema}
import edp.wormhole.util.HttpUtils
import edp.wormhole.util.httpclient.{HttpResult, HttpClientService}
import org.apache.http.entity.ContentType
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, StructType}


/**
  * Created by neo_qiang on 2020/5/5.
  */
class CollectionToBdp extends SwiftsInterface with EdpLogging{

  override def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, param:String, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String): DataFrame = {
    log.info("BdpConfig String:"+param)
    val bdpConfig:BdpConfig = BdpUtil.getBdpConfig(param)
    val schema = getSchema(bdpConfig.allmassage)
    val httpData = df.rdd.mapPartitions(p => {
      val httpClient = new HttpClientService()
      p.map(r => {
        val urlStr:String = BdpUtil.getAllUrlFromBdpConfig(bdpConfig,r.getAs[String](bdpConfig.key))
        log.info("Call BDP URL:"+urlStr)
        val rowMap:Map[String,String] = r.getValuesMap(r.schema.fieldNames)
        val request:HttpResult = httpClient.doPostCommon(urlStr,HttpUtils.getCommonHeader(),HttpUtils.dataToJsonString(rowMap.toSeq),ContentType.APPLICATION_JSON,false)
        log.info("Result String:"+request.getData)
        getROW(getDataFromRequest(request.getData,bdpConfig.allmassage),schema)
      })
    })
    val httpDf = session.createDataFrame(httpData,schema)
    if(config.datasetShow.getOrElse(false)) {
      httpDf.show(config.datasetShowNum.getOrElse(10))
    }
    df.join(httpDf,df.col(bdpConfig.key)=== httpDf.col(getJoinKey(bdpConfig)))
  }

  def getJoinKey(bdpConfig: BdpConfig):String={
    var returnString = bdpConfig.joinKey
    bdpConfig.allmassage.foreach(f=> {
      if (bdpConfig.joinKey == f.key) {returnString = getAsName(f)}
    })
    returnString
  }



  def getAsName(mas:Massagepath):String={
    if(mas.asName.getOrElse("").isEmpty) mas.key else mas.asName.get
  }

  def getDataFromRequest(data:String ,keySeq:Seq[Massagepath]):Seq[(String,String,StructField)] ={
     for(k<-keySeq) yield (
       getAsName(k),
       getMessageValue(data,k.path),
       StructField(
         getAsName(k),
         SparkSchemaUtils.ums2sparkType(UmsFieldType.umsFieldType(k.dataType)),
         k.nullable.getOrElse(true)
       )
       )
  }

  def getROW(oneRow:Seq[(String,String,StructField)],schema:StructType):Row={
    new GenericRowWithSchema(
      (for(k <- oneRow) yield SparkSchemaUtils.toTypedValue(k._2,k._3.dataType)).toArray
      ,schema)
  }


  def getSchema(allmessage:Seq[Massagepath]):StructType={
    StructType(
      for (k <- allmessage)
        //yield StructField(k.key,SparkSchemaUtils.ums2sparkSchema(UmsSchema(k.dataType)),k.nullable)
        yield StructField(
          getAsName(k),
          SparkSchemaUtils.ums2sparkType(UmsFieldType.umsFieldType(k.dataType)),
          k.nullable.getOrElse(true)
        )
    )
  }

  def getMessageValue(data:String,path:String): String ={
    val pathNode = path.split('.')
    var dataJson = JSON.parseObject(data)
    var tempString  = ""
    for(i <- 0 until pathNode.length ){
      val tempTuple = isArray(pathNode(i))
      if (tempTuple._1){
        dataJson = dataJson.getJSONArray(getNodeName(pathNode(i))).getJSONObject(tempTuple._2)
      }else if(i!=pathNode.length-1){
        dataJson = dataJson.getJSONObject(pathNode(i))
      }else{
        tempString =  dataJson.getString(pathNode(i))
      }
    }
    tempString
  }

  def isArray(nodeString:String):(Boolean,Int)={
    if(nodeString.endsWith("]")) (true,getIndex(nodeString)) else (false,-1)
  }

  def getIndex(node:String):Int={
    node.substring(node.indexOf('[')+1,node.length-1).toInt
  }

  def getNodeName(node:String):String={
    node.substring(0,node.indexOf('['))
  }

}

