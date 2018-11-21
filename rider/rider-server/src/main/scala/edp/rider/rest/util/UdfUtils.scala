package edp.rider.rest.util

import edp.rider.RiderStarter.modules
import edp.rider.common.{RiderConfig, RiderLogger}
import edp.rider.rest.persistence.entities._
import edp.rider.rest.util.CommonUtils.{currentMicroSec, currentSec, minTimeOut}
import edp.rider.rest.util.StreamUtils.riderLogger
import edp.rider.zookeeper.PushDirective
import edp.wormhole.ums.UmsProtocolType.DIRECTIVE_UDF_ADD
import edp.wormhole.util.JsonUtils

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await
import scala.sys.process._
import scala.language.postfixOps

object UdfUtils extends RiderLogger {
  def checkHdfsPathExist(jarName: String): Boolean = {
    val path = s"${RiderConfig.udfRootPath}/$jarName"
    try {
      val process = s"hdfs dfs -test -e $path" !
      val result = if (process.toInt == 0) true else false
      riderLogger.error(s"test hdfs path $path result: $process")
      result
    } catch {
      case ex: Exception =>
        riderLogger.error(s"check hdfs $path does exist failed", ex)
        false
    }
  }

  def deleteHdfsPath(jarName: String): Boolean = {
    val path = s"${RiderConfig.udfRootPath}/$jarName"
    try {
      if (checkHdfsPathExist(jarName)) {
        val process = s"hdfs dfs -rm -r $path" !
        val result = if (process.toInt == 0) true else false
        result
      }
      else false
    } catch {
      case ex: Exception =>
        riderLogger.error(s"delete hdfs $path failed", ex)
        false
    }
  }

  def sendUdfDirective(streamId: Long, udfSeq: Seq[StreamUdfResponse], userId: Long) = {
    try {
      val directiveSeq = new ArrayBuffer[Directive]
      udfSeq.foreach({
        udf =>
          val tuple = Seq(streamId, currentMicroSec, udf.functionName, udf.fullClassName, udf.jarName).mkString("#")
          directiveSeq += Directive(0, DIRECTIVE_UDF_ADD.toString, streamId, 0, tuple, "", currentSec, userId)
      })
      val directives: Seq[Directive] =
        if (directiveSeq.isEmpty) directiveSeq
        else Await.result(modules.directiveDal.insert(directiveSeq), minTimeOut)

      directives.foreach({
        directive =>
          val udfInfo = directive.directive.split("#")
          val msg =
            s"""
               |{
               |  "protocol": {
               |    "type": "${DIRECTIVE_UDF_ADD.toString}"
               |  },
               |  "schema": {
               |    "namespace": "",
               |    "fields": [
               |      {
               |        "name": "directive_id",
               |        "type": "long",
               |        "nullable": false
               |      },
               |      {
               |        "name": "stream_id",
               |        "type": "long",
               |        "nullable": false
               |      },
               |      {
               |        "name": "ums_ts_",
               |        "type": "datetime",
               |        "nullable": false
               |      },
               |      {
               |        "name": "udf_name",
               |        "type": "string",
               |        "nullable": false
               |      },{
               |        "name": "udf_class_fullname",
               |        "type": "string",
               |        "nullable": false
               |      },
               |      {
               |        "name": "udf_jar_path",
               |        "type": "string",
               |        "nullable": true
               |      }
               |    ]
               |  },
               |  "payload": [
               |    {
               |      "tuple": [${directive.id}, ${udfInfo(0)}, "${udfInfo(1)}", "${udfInfo(2)}", "${udfInfo(3)}", "${RiderConfig.udfRootPath}/${udfInfo(4)}"]
               |    }
               |  ]
               |}
               |
          """.stripMargin.replaceAll("\\n", "")
          riderLogger.info(s"user $userId send ${DIRECTIVE_UDF_ADD.toString} directive $msg")
          PushDirective.sendUdfDirective(streamId, udfInfo(2), JsonUtils.jsonCompact(msg))
      })
      riderLogger.info(s"user $userId send ${DIRECTIVE_UDF_ADD.toString} directives success.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"send stream $streamId udf directive failed", ex)
        throw ex
    }
  }

  //  def removeUdfDirective(streamId: Long, function: Option[String] = None, userId: Long): Unit = {
  //    try {
  //      PushDirective.removeUdfDirective(streamId, RiderConfig.zk, function)
  //      riderLogger.info(s"user $userId remove udf directive success.")
  //    } catch {
  //      case ex: Exception =>
  //        riderLogger.error(s"user $userId remove udf directive failed", ex)
  //        throw ex
  //    }
  //  }

  def removeUdfDirective(streamId: Long, userId: Long): Unit = {
    try {
      PushDirective.removeUdfDirective(streamId)
      riderLogger.info(s"user $userId remove udf directive success.")
    } catch {
      case ex: Exception =>
        riderLogger.error(s"user $userId remove udf directive failed", ex)
        throw ex
    }
  }

}
