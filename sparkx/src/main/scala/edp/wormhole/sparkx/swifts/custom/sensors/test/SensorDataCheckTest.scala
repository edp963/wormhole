package edp.wormhole.sparkx.swifts.custom.sensors.test

import edp.wormhole.sparkx.swifts.custom.sensors.SensorsDataTransform
import edp.wormhole.sparkxinterface.swifts.{SparkConfig, WormholeConfig}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object SensorDataCheckTest extends App {

  val streamDuration = 10
  //val master = "yarn"
  val master = "local[2]"
  val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName("testApp")
  val sparkContext: SparkContext = new SparkContext(sparkConf) //sparkCore
  val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate() //sql
  //val streamingContext = new StreamingContext(sparkConf, Seconds(streamDuration))
  //val streamingContext: StreamingContext = new StreamingContext(sparkContext, Seconds(streamDuration)) //streaming
  import sparkSession.sqlContext.implicits._


  val properties = "\"{\\\"$element_content\\\":\\\"精选\\\",\\\"$device_id\\\":\\\"EC2D146F-D5A6-418B-BEC7-84E5F23FA7B6\\\",\\\"$os_version\\\":\\\"13.3.1\\\",\\\"$carrier\\\":\\\"中国移动\\\",\\\"$os\\\":\\\"iOS\\\",\\\"$screen_height\\\":896,\\\"$is_first_day\\\":false,\\\"$screen_width\\\":414,\\\"$model\\\":\\\"iPhone11,8\\\",\\\"$lib\\\":\\\"iOS\\\",\\\"platform_type\\\":\\\"APP\\\",\\\"$app_version\\\":\\\"7.14.5\\\",\\\"$manufacturer\\\":\\\"Apple\\\",\\\"$network_type\\\":\\\"WIFI\\\",\\\"$wifi\\\":true,\\\"$screen_name\\\":\\\"KKTabViewController\\\",\\\"$element_type\\\":\\\"UITabBarItem\\\",\\\"$lib_version\\\":\\\"1.11.14\\\",\\\"$ip\\\":\\\"111.199.58.88\\\",\\\"$is_login_id\\\":true,\\\"$city\\\":\\\"北京\\\",\\\"$province\\\":\\\"北京\\\",\\\"$country\\\":\\\"中国\\\"}\""
  val lib = "\"{\\\"$lib_detail\\\":\\\"KKTabViewController######\\\",\\\"$lib_version\\\":\\\"1.11.14\\\",\\\"$lib\\\":\\\"iOS\\\",\\\"$lib_method\\\":\\\"autoTrack\\\",\\\"$app_version\\\":\\\"7.14.5\\\"}\""
  val extractor = "\"{\\\"f\\\":\\\"(dev=802,ino=1076244636)\\\",\\\"o\\\":4560603,\\\"n\\\":\\\"access_log.2020040214\\\",\\\"s\\\":188945173,\\\"c\\\":188945187,\\\"e\\\":\\\"data3.yx.sa\\\"}\""
  val json = """{"time":1585807824725,"_track_id":1151408659,"event":"$AppClick","_flush_time":1585807825834,"distinct_id":"z+Ay0hqnHWx3JcDNQ6jxQMjbb31qjaElyqjf26b/O67QmZGCpQNeKEvPn7WPLmvT","properties":""" + properties + ""","type":"track","lib":""" + lib + ""","map_id":"EC2D146F-D5A6-418B-BEC7-84E5F23FA7B6","user_id":-8372487356664007910,"recv_time":1585807825943,"extractor":""" + extractor + ""","project_id":15,"project":"default","ver":2}"""
  val ds: Dataset[String] = sparkSession.createDataset(Seq(json))
  val df=sparkSession.read.json(ds)
  df.show()


  val sourceNs = "kafka.kafka01022.topicl.ums.*.*.*"
  val sinkNs = ""
  val param:String="{\"projectId\":15,\"mysqlConnUrl\":\"jdbc:mysql://hdp1:3306/test\",\"mysqlUser\":\"root\",\"mysqlPassword\":\"root\",\"mysqlDatabase\":\"test\",\"duration\":0,\"dataTypeConst\":[\"0\",\"\",\"\",\"0\",\"0\",\"-1\"]}"
  val config:WormholeConfig =WormholeConfig(null,null,SparkConfig(1L,"test","test",0),1,"10.143.131.113:2181,10.143.131.119:2181,10.143.131.119:2181","/wormhole/lwl",false,Some(""),Some(""),Some(""),false,Some(false),None)
  val transform:SensorsDataTransform=new SensorsDataTransform()
  val dataFrame=transform.transform(sparkSession,df,null,param,config,sourceNs,sinkNs)
  dataFrame.show(100000)

}
