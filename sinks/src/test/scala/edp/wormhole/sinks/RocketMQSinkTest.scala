package edp.wormhole.sinks

import edp.wormhole.dbdriver.rocketmq.WormholeRocketMQConsumer
import edp.wormhole.publicinterface.sinks.SinkProcessConfig
import edp.wormhole.sinks.kudusink.Data2KuduSink
import edp.wormhole.sinks.rocketmqsink.{Data2RocketMQSink, DataJson2RocketMQSink}
import edp.wormhole.ums.UmsFieldType
import edp.wormhole.ums.UmsFieldType.UmsFieldType
import edp.wormhole.util.config.ConnectionConfig

import scala.collection.mutable

object RocketMQSinkTest extends App {
  dataJson2RocketMQSinkTest
  def data2RocketMQSinkTest() {
    val nameServer = ""
    val sourceNamespace = "kafka.kafka49.topicl.ums.*.*.*"
    val sinkNamespace = "rocketmq.mqtest.test_lwl.test.*.*.*"
    val specialConfig = s"""{"producerGroup":"test_lwl_producer","format":"flattenJson","preserveSystemField":true}"""
    //val specialConfig = s"""{"producerGroup":"test_lwl_producer","format":"ums","preserveSystemField":true}"""
    val sinkProcessConfig = SinkProcessConfig("", None, Some(specialConfig), None, "", 1, 100)
    //2020-04-26 14:36:35 [streaming-job-executor-0] INFO  edp.wormhole.sparkx.batchflow.BatchflowMainProcess[54] - d0dbd5c0-3ebd-48f9-8f71-7a021e4c3cca,kudu.kudu31.c31p130_default::.kududoubletest4.*.*.* schemaMap:Map(value4 -> (4,float,true), ums_id_ -> (1,long,true), ums_op_ -> (2,string,true), value2 -> (3,int,true), ums_ts_ -> (0,datetime,true))
    val schemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
    schemaMap.put("ums_ts_", (0, UmsFieldType.LONG, true))
    schemaMap.put("ums_id_", (1, UmsFieldType.LONG, true))
    schemaMap.put("ums_op_", (2, UmsFieldType.STRING, true))
    schemaMap.put("id", (3, UmsFieldType.DECIMAL, true))
    //schemaMap.put("value2", (4,UmsFieldType.DATETIME, true))
    schemaMap.put("value2", (4, UmsFieldType.INT, true))

    val tuple1 = Seq("1587898984", "3", "i", "10.10", null)
    val tupleList = Seq(tuple1)
    val connectionConfig = ConnectionConfig(nameServer, None, None, None)

    new Data2RocketMQSink().process(sourceNamespace, sinkNamespace, sinkProcessConfig, schemaMap, tupleList, connectionConfig)
    WormholeRocketMQConsumer.initConsumer(nameServer, "test_lwl_consumer", "test_lwl")
    println("end")
  }


  def dataJson2RocketMQSinkTest() {
    val nameServer = ""
    val sourceNamespace = "kafka.kafka49.topicl.ums.*.*.*"
    val sinkNamespace = "rocketmq.mqtest.test_lwl.test.*.*.*"
    val specialConfig = s"""{"producerGroup":"test_lwl_producer","format":"flattenJson","preserveSystemField":true}"""
    //val specialConfig = s"""{"producerGroup":"test_lwl_producer","format":"ums","preserveSystemField":true}"""
    val jsonSchema = s"""{"fields":[{"name":"ums_op_","type":"string","nullable":true},{"name":"value2","type":"int","nullable":true},{"name":"ums_ts_","type":"long","nullable":true},{"name":"ums_id_","type":"long","nullable":true},{"name":"id","type":"decimal","nullable":true}]}"""
    val sinkProcessConfig = SinkProcessConfig("", None, Some(specialConfig), Some(jsonSchema), "", 1, 100)
    //2020-04-26 14:36:35 [streaming-job-executor-0] INFO  edp.wormhole.sparkx.batchflow.BatchflowMainProcess[54] - d0dbd5c0-3ebd-48f9-8f71-7a021e4c3cca,kudu.kudu31.c31p130_default::.kududoubletest4.*.*.* schemaMap:Map(value4 -> (4,float,true), ums_id_ -> (1,long,true), ums_op_ -> (2,string,true), value2 -> (3,int,true), ums_ts_ -> (0,datetime,true))
    val schemaMap = mutable.HashMap.empty[String, (Int, UmsFieldType, Boolean)]
    schemaMap.put("ums_ts_", (0, UmsFieldType.LONG, true))
    schemaMap.put("ums_id_", (1, UmsFieldType.LONG, true))
    schemaMap.put("ums_op_", (2, UmsFieldType.STRING, true))
    schemaMap.put("id", (3, UmsFieldType.DECIMAL, true))
    //schemaMap.put("value2", (4,UmsFieldType.DATETIME, true))
    schemaMap.put("value2", (4, UmsFieldType.INT, true))

    val tuple1 = Seq("1587898984", "3", "i", "10.10", null)
    val tupleList = Seq(tuple1)
    val connectionConfig = ConnectionConfig(nameServer, None, None, None)

    new DataJson2RocketMQSink().process(sourceNamespace, sinkNamespace, sinkProcessConfig, schemaMap, tupleList, connectionConfig)
    WormholeRocketMQConsumer.initConsumer(nameServer, "test_lwl_consumer", "test_lwl")
    println("end")
  }

}
