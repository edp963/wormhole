package edp.wormhole.sparkx.swifts.custom.sensors.test

import com.alibaba.fastjson.{JSON, JSONObject}
import edp.wormhole.sparkx.swifts.custom.sensors.ConvertUtils
import edp.wormhole.sparkx.swifts.custom.sensors.entry.PropertyColumnEntry
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StringType
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by IntelliJ IDEA.
  *
  * @Author daemon
  * @Date 19/11/20 19:03
  *       To change this template use File | Settings | File Templates.
  */
object SensorsDataTest  extends App {


  val streamDuration = 10
  //val master = "yarn"
  val master = "local[2]"
  val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName("testApp")
  val sparkContext: SparkContext = new SparkContext(sparkConf) //sparkCore
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate() //sql
  //val streamingContext = new StreamingContext(sparkConf, Seconds(streamDuration))
  val streamingContext: StreamingContext = new StreamingContext(sparkContext, Seconds(streamDuration)) //streaming
  import sparkSession.sqlContext.implicits._
  val json1 = """{"a":1, "b": "23.1", "c": 1,"d":{"x":1,"y":"b"}}"""
  val json2 = """{"a":2, "b": "hello", "c": 1.2,"d":{"x":2,"y":"m"}}"""

  val txt: Dataset[String] = sparkSession.read.text("/Users/creditease/Downloads/dataworks/kafka.json").as[String](Encoders.STRING);
  txt.show(10000)
  //val jsonList=mutable.ListBuffer[String];

  val df=sparkSession.read.json(txt.map(x=>x))

  val df1=df.withColumn("f1",lit(null).cast(StringType))

  val time=df1.agg("time"->"max").first().getAs[Long](0);

  println(time)

  df1.dropDuplicates(Array("_flush_time")).show(10000)

  //val df=sparkSession.read.json("/Users/creditease/Downloads/dataworks/kafka1.json")
//  df.show();
//
//  val t:SensorsDataTransform=new SensorsDataTransform();
//
//  val sourceNs = ""
//  val sinkNs = ""
//  val param:String="{\n\"projectId\":1,\n\"mysqlConnUrl\":\"jdbc:mysql://10.143.252.121:3306/metadata\",\n\"mysqlUser\":\"test121\",\n\"mysqlPassword\":\"dds@test121\",\n\"mysqlDatabase\":\"metadata\",\n\"clickHouseConnUrl\":\"jdbc:clickhouse://10.143.151.220:8123\",\n\"clickHouseUser\":\"root\",\n\"clickHousePassword\":\"root\",\n\"clickHouseDatabase\":\"event_data\",\n\"clickHouseTableName\":\"event_wos_p1\",\n\"clickHouseCluster\":\"bip_ck_cluster\"\n}"
//  val config:WormholeConfig =WormholeConfig(null,null,SparkConfig(1L,"test","test",0),1,"10.143.131.113:2181,10.143.131.119:2181,10.143.131.119:2181","/wormhole",false,Some(""),Some(""),Some(""),false,Some(false),None)
//  val dataFrame=t.transform(sparkSession,df,null,param,config,sourceNs,sinkNs)
//  dataFrame.show(100000)

  //val ds = sparkSession.createDataset(Seq(json1, json2))

  //val df=sparkSession.read.json(ds);

  //df.printSchema()
  //df.show(10)

  //df.foreach(row=>{
   //val s= row.getStruct(row.fieldIndex("d"))
    //val map=row.getAs[GenericRowWithSchema](row.fieldIndex("d"))

    //println(s)
    //println(s.length)
    //
    //s.schema.fieldNames.foreach(x=>print(x))
    //val r=row.getAs[GenericRowWithSchema]("d")
    //println(r.)
//    val resultList = Seq[Any]
//
//    resultList.;
//    resultList+=2;
//    resultList+="4a"
    //row.get(0)

    //val array=Array(row.get(0),2,"4a");
//    array.

    //val schema=StructType(Seq(StructField("f1",IntegerType,true),StructField("f2",IntegerType,true),StructField("f3",StringType,true)))

    //resultList+=new GenericRowWithSchema(array,schema);

  //})

//  val resultRowRdd: RDD[Row] = df.rdd.mapPartitions(it=>{
//    val resultList = mutable.ListBuffer.empty[Row]
//    //    array.
//
//    val listRow:List[Row]=it.toList
//    listRow.foreach(row=>{
//      val array=Array(1,2,"4a");
//      val schema=StructType(Seq(StructField("f1",IntegerType,true),StructField("f2",IntegerType,true),StructField("f3",StringType,true)))
//      resultList+=new GenericRowWithSchema(array,schema);
//    })
//
//
//    resultList.iterator;
//  })
//
//  val ab=ArrayBuffer[StructField]();

  //ab+="Hello"
  //ab+=2
//  ab+=StructField("f1",IntegerType,true)
//  ab+=StructField("f2",IntegerType,true)
//  ab+=StructField("f3",StringType,true)

 // val schema=StructType(Seq(StructField("f1",IntegerType,true),StructField("f2",IntegerType,true),StructField("f3",StringType,true)))

  //val sc=StructType(ab.toSeq)

//  val dframe=sparkSession.createDataFrame(resultRowRdd,StructType(ab.toSeq));
//  dframe.show(100)

  //df.show(100)



  //df.toJSON.foreach(x=>println(x))
  //streamingContext.start()
  //streamingContext.awaitTermination()
  def  f:Unit={
    println("00000000")
  }

}


object MyTest extends App{

  /*val clazz=Class.forName("edp.wormhole.sparkx.swifts.custom.DiffBU")

 val ms:Array[Method]=clazz.getDeclaredMethods()
  val tm= ms.filter(x=>x.getName().equals("transform"))
  for(m<-tm){
    println(m.getParameterCount)
    m.getParameterTypes.foreach(x=>println(x.toString))
    //m.getParameterTypes

  }*/

  val json = "{\"$device_id\":\"6B5F9AD0-B401-4ED0-933A-96B4CFF64BAA\",\"$os_version\":\"13.1.3\",\"$carrier\":\"中国移动\",\"$os\":\"iOS\",\"$screen_height\":896,\"$is_first_day\":false,\"$screen_width\":414,\"$model\":\"iPhone11,8\",\"$lib\":\"iOS\",\"platform_type\":\"APP\",\"$app_version\":\"7.9.1\",\"$manufacturer\":\"Apple\",\"$network_type\":\"WIFI\",\"$wifi\":true,\"$lib_version\":\"1.11.14\",\"$ip\":\"218.247.253.242\",\"$event_duration\":18.43,\"$is_login_id\":true,\"$city\":\"北京\",\"$province\":\"北京\",\"$country\":\"中国\"}"
  val jsonObj: JSONObject=JSON.parseObject(json)
  val key = "$event_duration"
  val _value:Object=jsonObj.get(key)
  val colunm = new PropertyColumnEntry()
  colunm.setColumn_name("Int64")
  colunm.setData_type(1)
  val rowValue =ConvertUtils.convert(key,colunm,_value)
  println(rowValue)

}



