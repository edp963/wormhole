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


package edp.wormhole.sinks.utils

import edp.wormhole.externalclient.hadoop.HdfsUtils._
import edp.wormhole.ums.{UmsNamespace, UmsWatermark}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, PathFilter}
import edp.wormhole.util.DateUtils._
import edp.wormhole.util.FileUtils._
import edp.wormhole.util.CommonUtils._
import edp.wormhole.util.DtFormat

import scala.collection.mutable
import scala.collection.mutable.ListBuffer



case class SinkHdfsLogPath(root: String, ns: UmsNamespace, folder: SinkHdfsLogPathFolder) {
  private val base = SinkHdfsLogPathBase.STG.toString
  private val nsStr = UmsNamespace.namespacePath(ns)

  lazy val nsPath = List(
    pf(root),
    pf(base),
    pf(nsStr)).mkString("/").toLowerCase

  lazy val fullPath = List(
    pf(nsPath),
    pf(List(yyyyMMddHHmmss(folder.minTs.ts), yyyyMMddHHmmss(folder.maxTs.ts), folder.count,
      currentyyyyMMddHHmmss).mkString("_"))).mkString("/").toLowerCase
}

object SinkHdfsLogPathBase extends Enumeration {
  type WhHdfsLogPathBase = Value

  val STG = Value("stg")
  val ODS = Value("ods")
  val DW = Value("dw")
  val APP = Value("app")
  val USER = Value("user")

  def whHdfsLogPathBase(s: String) = SinkHdfsLogPathBase.withName(s.toLowerCase)
}

case class SinkHdfsLogPathFolder(minTs: UmsWatermark, maxTs: UmsWatermark, count: Int)

object SinkHdfsLogPathUtils extends SinkHdfsLogPathUtils


trait SinkHdfsLogPathUtils {
  def whHdfsLogPathFolder(fullPath: String): SinkHdfsLogPathFolder = {
    assert(fullPath != null && fullPath.trim.length > 0, "The path does not exist")
    val lastArray = fullPath.split("/").last.split("_")
    //    WhHdfsLogPathFolder(UmsWatermark(lastArray(0), s2long(lastArray(1), 0L)), s2int(lastArray(2), 0))
    //    WhHdfsLogPathFolder(UmsWatermark(lastArray(0)), s2int(lastArray(1), 0))

    val minTs = if (lastArray(0).startsWith(edpTempPrefix)) lastArray(0).drop(edpTempPrefix.length) else lastArray(0)
    val maxTs = lastArray(1)
    SinkHdfsLogPathFolder(UmsWatermark(minTs), UmsWatermark(maxTs), s2int(lastArray(2), 0))
  }

  def getHdfsLogPathListBetween(fullPathList: List[String], fromTsStr: String = null, toTsStr: String = null): List[String] = {
    //TODO: there is no directory that more than toTs
    val fromTs = dt2dateTime(fromTsStr)
    val toTs = dt2dateTime(toTsStr)

    fullPathList.filter(path => {
      var result = false
      if (fromTs == null && toTs == null) {
        result = true
      } else if (fromTs == null && toTs != null) {
        if (!toTs.isBefore(whHdfsLogPathFolder(path).minTs.ts))
          result = true
      } else if (fromTs != null && toTs == null) {
        if (!fromTs.isAfter(whHdfsLogPathFolder(path).maxTs.ts))
          result = true
      } else {
        if (!fromTs.isAfter(whHdfsLogPathFolder(path).maxTs.ts) && !toTs.isBefore(whHdfsLogPathFolder(path).minTs.ts))
          result = true
      }
      result
    })
    //    var fromIndex = 0
    //    var toIndex = fullPathList.length - 1
    //    var flag = true

    //    if (fromTs != null || toTs != null) {
    //      fullPathList.zipWithIndex.foreach {
    //        s =>
    //          val min_ts = whHdfsLogPathFolder(s._1).minTs.ts
    //          val max_ts = whHdfsLogPathFolder(s._1).maxTs.ts
    //          if (fromTs != null && max_ts.isBefore(fromTs)) fromIndex = s._2 + 1
    //          if (flag && toTs != null && min_ts.isAfter(toTs)) {
    //            toIndex = s._2 - 1
    //            flag = false
    //          }
    //        //          if (!flag && toTs != null && min_ts.isAfter(toTs) && min_ts == whHdfsLogPathFolder(fullPathList(toIndex)).minTs.ts)
    //        //            toIndex = s._2
    //      }
    //    }
    //    fullPathList.slice(fromIndex, toIndex + 1)
  }

  /**
    * get list of hdfslog full path given below condition
    * if fromTs is default(empty), from the very beginning
    * if toTs is default(empty), to the very end
    *
    * @param rootBaseNsPath hdfslog pathRoot + pathBase + pathNs
    * @param fromTsStr      hdfslog path that contains ts which greater and equal fromTs(yyyyMMddHHmmss)
    * @param toTsStr        hdfslog path that contains ts which less and equal toTs(yyyyMMddHHmmss)
    * @return list of           hdfslog full path required
    */
  //  def getHdfsLogPathList(rootBaseNsPath: String, fromTsStr: String = null, toTsStr: String = null, exclude0cnt: Boolean = true): List[String] = {
  //    val config = new Configuration() // TODO: where to config Configuration, should pass from env?
  //
  //    val fullRootBaseNsPath = pf(rootBaseNsPath) //TODO: will consider support dbPar/tablePar later
  //    assert(isPathExist(config, fullRootBaseNsPath), s"The $fullRootBaseNsPath does not exist")
  //
  //    val pathList = FileSystem.newInstance(config).listStatus(new Path(fullRootBaseNsPath), new PathFilter() {
  //      override def accept(path: Path): Boolean = {
  //        if (!isPathReady(config, path)) return false
  //        if (exclude0cnt && whHdfsLogPathFolder(path.toString).count == 0) return false
  //        true
  //      }
  //    }).map(_.getPath.toString).sortWith((p1, p2) =>
  //      //      whHdfsLogPathFolder(p1).wm.id < whHdfsLogPathFolder(p2).wm.id
  //      whHdfsLogPathFolder(p1).wm.ts.isBefore(whHdfsLogPathFolder(p2).wm.ts)).toList
  //
  //    getHdfsLogPathListBetween(pathList, fromTsStr, toTsStr)
  //  }
  def getHdfsLogPathList(rootBaseNsPath: String,
                         fromTsStr: String = null,
                         toTsStr: String = null,
                         exclude0cnt: Boolean = true): List[String] = {
    val config = new Configuration()
    val filesystem = FileSystem.newInstance(config)
    val fullRootBaseNsPath = pfRight(rootBaseNsPath)
    assert(isPathExist(config, fullRootBaseNsPath), s"The $fullRootBaseNsPath does not exist")
    val databaseParList = filesystem.listStatus(new Path(fullRootBaseNsPath)).map(_.getPath.toString).toList
    val tableParList = ListBuffer[String]()
    databaseParList.foreach(path =>
      tableParList.append(filesystem.listStatus(new Path(path)).map(_.getPath.toString): _*)
    )
    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = {
        val p = path.toString
        //        if (!isValidWhPath(p)) return false
        if (isEdpTempPath(p)) return false
        if (!isPathExist(config, p)) return false
        if (exclude0cnt && whHdfsLogPathFolder(p).count == 0) return false
        true
      }
    }

    val pathList = ListBuffer[String]()
    tableParList.toList.foreach(path => {
      pathList.append(filesystem.listStatus(new Path(path), pathFilter)
        .map(_.getPath.toString): _*)
    })
    getHdfsLogPathListBetween(pathList.toList, fromTsStr, toTsStr)
  }

  /**
    * get all pathRootBaseNs of pathRootBase
    *
    * @param rootBase hdfslog pathRoot + pathBase
    * @return all pathRootBaseNs required
    */
  def getRootBaseNsList(rootBase: String): List[String] = {
    def getRootBaseNsArray(conf: Configuration, path: Path, depth: Int): Array[String] = {
      if (depth < 7) FileSystem.get(conf).listStatus(path).flatMap(p => getRootBaseNsArray(conf, p.getPath, depth + 1))
      else FileSystem.get(conf).listStatus(path).map(_.getPath.toString)
    }
    val conf = new Configuration
    if (isPathExist(conf, rootBase)) getRootBaseNsArray(conf, new Path(rootBase), 1).toList
    else List.empty
  }

  def getCompactionPathList(rootBaseNsPath: String, exclude0cnt: Boolean = true): List[String] = {
    val config = new Configuration()

    val fullRootBaseNsPath = pfRight(rootBaseNsPath) //TODO: will consider support dbPar/tablePar later
    assert(isPathExist(config, fullRootBaseNsPath), s"The $fullRootBaseNsPath does not exist")

    val pathFilter = new PathFilter {
      override def accept(path: Path): Boolean = {
        if (!isValidWhPath(path.toString)) return false
        if (isEdpTempPath(path.toString)) return true
        if (!isPathExist(config, path)) return false
        if (exclude0cnt && whHdfsLogPathFolder(path.toString).count == 0) return false
        true
      }
    }

    val pathList = FileSystem.newInstance(config).listStatus(new Path(fullRootBaseNsPath), pathFilter)
      .map(_.getPath.toString).sortWith((p1, p2) =>
      whHdfsLogPathFolder(p1).minTs.ts.isBefore(whHdfsLogPathFolder(p2).minTs.ts)).toList

    getHdfsLogPathListBetween(pathList, null, null)
  }

  /**
    * get all paths of rootBaseNs whcih need to merge
    *
    * @param rootBaseNs hdfslog pathRoot + pathBase + pathNs
    * @return all paths need  to be merge under one namespace, every element of return List[List] (which is List)
    *         present paths need to merge in one day
    */
  //  def getCompactionPathListGroup(rootBaseNs: String): List[List[String]] = {
  //    def getPathMinDate(path: String): String = {
  //      val tmp = path.split("/").last
  //      if (tmp.startsWith(edpTempPrefix)) tmp.slice(edpTempPrefix.length, edpTempPrefix.length + 8) else tmp.take(8)
  //    }
  //
  //    val seqop = (rez: mutable.HashMap[String, List[String]], path: String) => {
  //      rez.update(getPathMinDate(path), path :: rez.getOrElse(getPathMinDate(path), Nil))
  //      rez
  //    }
  //    val combop = (rez1: mutable.HashMap[String, List[String]], rez2: mutable.HashMap[String, List[String]]) => rez1 ++= rez2
  //    val pathList = getCompactionPathList(rootBaseNs)
  //    if (pathList.isEmpty) return List.empty
  //    val pathListGroup = pathList.aggregate(mutable.HashMap[String, List[String]]())(seqop, combop)
  //    (pathListGroup - pathListGroup.keys.maxBy(dt2long)).values.toList.filter(_.length > 1)
  //  }

  def getCompactionPathListGroup(rootBaseNs: String): List[List[String]] = {
    val seqop = (rez: mutable.HashMap[String, List[String]], path: String) => {
      val processDate = path.split("/").last.split("_").last.take(8)
      rez.update(processDate, path :: rez.getOrElse(processDate, Nil))
      rez
    }
    val combop = (rez1: mutable.HashMap[String, List[String]], rez2: mutable.HashMap[String, List[String]]) => rez1 ++= rez2
    val pathList = getCompactionPathList(rootBaseNs)
    if (pathList.isEmpty) return List.empty
    val pathListGroup = pathList.aggregate(mutable.HashMap[String, List[String]]())(seqop, combop)
    (pathListGroup - currentyyyyMMddHHmmss.take(8)).values.toList.filter(_.length > 1)
  }

  def isEdpTempPath(path: String): Boolean = path.split("/").last.startsWith(edpTempPrefix)

  /** check whether the full path meets the wormhole format requirement */
  private def isValidWhPath(fullPath: String): Boolean = {
    val tmp = fullPath.split("/")
    if (tmp.length < 7) return false
    val path = tmp.last
    val directPath = if (path.startsWith(edpTempPrefix)) path.drop(edpTempPrefix.length) else path
    val lastArray = directPath.split("_")
    if (lastArray.length != 4 ||
      !isValidTimeFormat(lastArray(0), DtFormat.TS_NOD_SEC) ||
      !isValidTimeFormat(lastArray(1), DtFormat.TS_NOD_SEC) ||
      !isValidTimeFormat(lastArray(3), DtFormat.TS_NOD_SEC)) return false
    true
  }

}
