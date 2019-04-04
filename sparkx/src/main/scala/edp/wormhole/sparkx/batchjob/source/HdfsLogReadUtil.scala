/*-
 * <<
 * wormhole
 * ==
 * Copyright (C) 2016 - 2017 EDP
 * ==
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * >>
 */


package edp.wormhole.sparkx.batchjob.source

import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.ums.UmsProtocolType
import edp.wormhole.util.{DateUtils, FileUtils}
import edp.wormhole.externalclient.hadoop.HdfsUtils._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ListBuffer

object HdfsLogReadUtil extends EdpLogging {
  def getHdfsLogPathListBetween(conf: Configuration, fullPathList: Seq[String], fromTsStr: String, toTsStr: String): Seq[String] = {
    if (fromTsStr != null && fromTsStr.nonEmpty && toTsStr != null && toTsStr.nonEmpty) {
      val fromTsLong: Long = if (fromTsStr != null) DateUtils.dt2long(fromTsStr) else 0
      val toTsLong: Long = if (toTsStr != null) DateUtils.dt2long(toTsStr) else 0
      println("fromTsLong:" + fromTsLong + "         " + fromTsStr)
      println("toTsLong:" + toTsLong + "     " + toTsStr)
      val fileList = ListBuffer.empty[String]
      fullPathList.foreach(path => {
        val fileName = path.split("\\/").last
        if (fileName.startsWith("metadata")) {
          val fileContent = HdfsUtils.readFileString(conf, path)
          logInfo("****metadata,fileName=" + fileName + ",fileContent=" + fileContent)
          val contentGrp = fileContent.split("_")
          var result = false
          val minTs = contentGrp(3)
          val maxTs = contentGrp(4)
          if (fromTsStr == null && toTsStr == null) {
            result = true
          } else if (fromTsStr == null && toTsStr != null) {
            if (DateUtils.dt2long(minTs) <= toTsLong) result = true
          } else if (fromTsStr != null && toTsStr == null) {
            if (DateUtils.dt2long(maxTs) >= fromTsLong) result = true
          } else if (DateUtils.dt2long(minTs) <= fromTsLong && DateUtils.dt2long(maxTs) >= fromTsLong) {
            result = true
          } else if (DateUtils.dt2long(minTs) <= toTsLong && DateUtils.dt2long(maxTs) >= toTsLong) {
            result = true
          } else if (DateUtils.dt2long(minTs) >= fromTsLong && DateUtils.dt2long(maxTs) <= toTsLong) {
            result = true
          }

          if (result) {
            fileList += (path.substring(0, path.lastIndexOf("/")) + "/" + contentGrp(0))
          }
        }
      })
      fileList
    } else fullPathList
  }

  def getHdfsPathList(conf: Configuration, hdfsRoot: String, namespace: String, protocolTypeSet: Set[String]): Seq[String] = {
    val names = namespace.split("\\.")

    var prefix = hdfsRoot + "/hdfslog/" + names(0) + "." + names(1) + "." + names(2) + "/" + names(3)
    //+ "/" + namespace.version + "/" + namespace.databasePar + "/" + namespace.tablePar + "/" + "protocoltype/right"
    val namespaceVersion = if (names(4) == "*") {
      getHdfsFileList(conf, prefix).map(t => t.substring(t.lastIndexOf("/") + 1).toInt).sortWith(_ > _).head.toString
    } else {
      names(4)
    }
    val pathList = ListBuffer.empty[String]
    prefix = prefix + "/" + namespaceVersion
    val parentPath = getHdfsFileList(conf, prefix).flatMap(getHdfsFileList(conf, _))

    if (protocolTypeSet.contains(UmsProtocolType.DATA_INITIAL_DATA.toString))
      parentPath.map(pathList += _ + "/" + UmsProtocolType.DATA_INITIAL_DATA.toString + "/right")
    if (protocolTypeSet.contains(UmsProtocolType.DATA_INCREMENT_DATA.toString))
      parentPath.map(pathList += _ + "/" + UmsProtocolType.DATA_INCREMENT_DATA.toString + "/right")

    pathList.toList
  }

  def getHdfsFileList(config: Configuration, hdfsPath: String): Seq[String] = {
    val fileSystem = FileSystem.newInstance(config)
    val fullPath = FileUtils.pfRight(hdfsPath)
    assert(isPathExist(config, fullPath), s"The $fullPath does not exist")
    fileSystem.listStatus(new Path(fullPath)).map(_.getPath.toString).toList
  }

  def getHdfsFileList(config: Configuration, hdfsPathList: Seq[String]): Seq[String] = {
    val fileSystem = FileSystem.newInstance(config)
    val fullPathList: Seq[String] = hdfsPathList.map(FileUtils.pfRight(_))
    val checkedPathList = fullPathList.filter(fullPath => {
      val exist = isPathExist(config, fullPath)
      if (!exist) logError("path is not exist,path=" + fullPath)
      else logInfo("path exist,path=" + fullPath)
      exist
    })
    checkedPathList.flatMap(fullPath => fileSystem.listStatus(new Path(fullPath)).map(_.getPath.toString))
  }

  //  def getSnapshotSqlByTs(keys: String, fromTs: Timestamp, toTs: Timestamp): String = {
  //    s"""
  //       |select * from
  //       |    (select *, row_number() over
  //       |      (partition by $keys order by ${UmsSysField.ID.toString} desc) as rn
  //       |    from increment
  //       |      where ${UmsSysField.TS.toString} >= '$fromTs' and ${UmsSysField.TS.toString} <= '$toTs')
  //       |    increment_filter
  //       |  where ${UmsSysField.OP.toString} != '${UmsOpType.DELETE.toString.toLowerCase}' and ${UmsSysField.OP.toString} != '${UmsOpType.DELETE.toString.toUpperCase()}' and rn = 1
  //          """.stripMargin.replace("\n", " ")
  //  }


}
