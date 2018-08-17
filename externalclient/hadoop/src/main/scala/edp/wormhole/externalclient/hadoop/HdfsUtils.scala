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


package edp.wormhole.externalclient.hadoop

import java.io._
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import scala.collection.mutable.ListBuffer

object HdfsUtils extends HdfsUtils


trait HdfsUtils {
  def isPathExist(config: Configuration, path: String): Boolean = {
    assert(path != null && path.trim.length > 0, "The given path string is null, please check it")
    FileSystem.get(config).exists(new Path(path))
  }

  def isPathExist(path: String): Boolean = {
    val config = new Configuration()
    isPathExist(config, path)
  }

  def isPathExist(config: Configuration, path: Path): Boolean = {
    assert(path != null, "The given path is null, please check it")
    isPathExist(config, path.toString)
  }

  def isParquetPathReady(config: Configuration, path: String): Boolean = isPathExist(config, path + "/_SUCCESS")

  //
  //  def isParquetPathReady(config: Configuration, path: Path): Boolean = {
  //    assert(path != null, "The given path is null, please check it")
  //    isParquetPathReady(config, path.toString)
  //  }
  //
  //  def waitForParquetPathReady(maxWaitTime: Long, waitStep: Long, paths: String*): Boolean = {
  //    def waitForPathReady(paths: mutable.Set[String], pastTime: Long): Boolean = {
  //      if (pastTime > maxWaitTime && paths.nonEmpty) false
  //      else {
  //        for (path <- paths) if (isParquetPathReady(new Configuration(), path)) paths -= path
  //        if (paths.isEmpty) true
  //        else {
  //          Thread.sleep(waitStep)
  //          waitForPathReady(paths, pastTime + waitStep)
  //        }
  //      }
  //    }
  //
  //    assert(null != paths && paths.nonEmpty, "paths cannot be null or empty")
  //    waitForPathReady(mutable.HashSet[String](paths: _*), 0L)
  //  }
  //
  //  def waitForPathReady(maxWaitTime: Long, paths: String*): Boolean = waitForParquetPathReady(maxWaitTime, 500L, paths: _*)
  //
  //  def waitForPathReady(paths: String*): Boolean = waitForPathReady(1800000L, paths: _*)

  def deletePath(paths: String*): Unit = {
    val configuration = new Configuration()
    deletePath(configuration, paths: _*)
  }

  def deletePath(configuration: Configuration, paths: String*): Unit = {
    val fs = FileSystem.newInstance(configuration)
    paths.foreach(path => {
      if (isPathExist(configuration, path))
        fs.delete(new Path(path), true)
    })
    fs.close()
  }

  def createPath(configuration: Configuration, path: String): Unit = {
    val fs = FileSystem.newInstance(configuration)
    if (!isPathExist(configuration, path))
      fs.create(new Path(path))
    fs.close()
  }

  def renamePath(from: String, to: String): Unit = {
    val conf = new Configuration()
    renamePath(conf, from, to)
  }


  def renamePath(configuration: Configuration, from: String, to: String): Unit = {
    val fs = FileSystem.newInstance(configuration)
    fs.rename(new Path(from), new Path(to))
    fs.close()
  }

  def writeFile(conf: Configuration, obj: Any, hdfsUrl: String): Unit = {
    //   val conf = new Configuration()
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)
    val path = new Path(hdfsUrl)

    val output = new ObjectOutputStream(fs.create(path))
    output.writeObject(obj)
    output.close()
    fs.close()
  }


  def writeString(obj: Any, hdfsUrl: String): Unit = {
    val conf = new Configuration()
    writeFile(conf, obj, hdfsUrl)
  }

  def readFile(hdfsUrl: String): AnyRef = {
    val conf = new Configuration()
    readFile(conf, hdfsUrl)
  }

  def listFile(hdfsUrl: String): Array[Path] = {
    val conf = new Configuration()
    listFile(conf, hdfsUrl)
  }

  def listFile(conf: Configuration, hdfsUrl: String): Array[Path] = {
    val fs = FileSystem.newInstance(conf)
    val files: Array[FileStatus] = fs.listStatus(new Path(hdfsUrl))
    val fileNames = files.map(file => {
      file.getPath
    })
    fs.close()
    fileNames
  }

  def readFile(conf: Configuration, hdfsUrl: String): AnyRef = {
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)
    val filePath = new Path(hdfsUrl)
    val openfile = new FSDataInputStream(fs.open(filePath))
    val objectMap = new ObjectInputStream(openfile)
    val readObject = objectMap.readObject()
    objectMap.close()
    fs.close()
    readObject
  }

  def readFileString(conf: Configuration, hdfsUrl: String): String = {
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)
    val filePath = new Path(hdfsUrl)
    val openfile = new FSDataInputStream(fs.open(filePath))
    val objectMap = new ObjectInputStream(openfile)
    val content = objectMap.readUTF()
    objectMap.close()
    fs.close()
    content
  }

  def readFile(conf: Configuration, path: Path): AnyRef = {
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)
    val openfile = new FSDataInputStream(fs.open(path))
    val objectMap = new ObjectInputStream(openfile)
    val readObject = objectMap.readObject()
    objectMap.close()
    fs.close()
    readObject
  }

  def overwriteFile(conf: Configuration, hdfsUrl: String, in: ByteArrayInputStream): Unit = {
    deletePath(conf, hdfsUrl)
    createPath(conf, hdfsUrl)
    appendToFile(conf, hdfsUrl, in)
  }

  def appendToFile(conf: Configuration, hdfsUrl: String, in: ByteArrayInputStream): Unit = {
    conf.setBoolean("dfs.support.append", true)
    val fs = FileSystem.newInstance(URI.create(hdfsUrl), conf)
    val out = fs.append(new Path(hdfsUrl))
    IOUtils.copyBytes(in, out, 60 * 1024 * 1024, true)
    fs.close()
  }

  def readFileByLineNum(txtFilePath: String, lineNum:Int): String = {
    readFileByLineNum(txtFilePath, new Configuration(), lineNum)
  }

  def readFileByLineNum(txtFilePath: String, conf: Configuration, lineNum:Int): String = {
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    var returnStr = ""
    var fsr: FSDataInputStream = null
    var bufferedReader: BufferedReader = null
    val fs = FileSystem.newInstance(conf)
    try {
      var lineStr: String = null
      var i = 1
      fsr = fs.open(new Path(txtFilePath))
      bufferedReader = new BufferedReader(new InputStreamReader(fsr))
      lineStr = bufferedReader.readLine
      while (lineStr != null) {
        if(i>=lineNum){
          returnStr = lineStr
          lineStr = null.asInstanceOf[String]
        }else{
          i += 1
          lineStr = bufferedReader.readLine
        }
      }
    } catch {
      case e: Exception =>
        println(e)
        throw e
    } finally {
      if (bufferedReader != null) {
        try
          bufferedReader.close()
        catch {
          case e: IOException =>
            println(e)
        }
      }
      if (fsr != null) {
        try
          fsr.close()
        catch {
          case e: IOException =>
            println(e)
        }
      }
      if (fs != null) {
        try
          fs.close()
        catch {
          case e: IOException =>
            println(e)
        }
      }
    }
    returnStr
  }

  def readStringByLine(txtFilePath: Path, conf: Configuration): Seq[String] = {
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val list = ListBuffer.empty[String]
    var fsr: FSDataInputStream = null
    var bufferedReader: BufferedReader = null
    val fs = FileSystem.newInstance(conf)
    try {
      var lineStr: String = null
      fsr = fs.open(txtFilePath)
      bufferedReader = new BufferedReader(new InputStreamReader(fsr))
      lineStr = bufferedReader.readLine
      while (lineStr != null) {
        list += lineStr
        lineStr = bufferedReader.readLine
      }
    } catch {
      case e: Exception =>
        println(e)
        throw e
    } finally {
      if (bufferedReader != null) {
        try
          bufferedReader.close()
        catch {
          case e: IOException =>
            println(e)
        }
      }
      if (fsr != null) {
        try
          fsr.close()
        catch {
          case e: IOException =>
            println(e)
        }
      }
      if (fs != null) {
        try
          fs.close()
        catch {
          case e: IOException =>
            println(e)
        }
      }
    }
    list
  }


  def writeString(conf: Configuration, obj: String, hdfsUrl: String): Unit = {
    //   val conf = new Configuration()
    conf.setBoolean("fs.hdfs.impl.disable.cache", true)
    val fs = FileSystem.newInstance(conf)
    val path = new Path(hdfsUrl)

    val output = new ObjectOutputStream(fs.create(path))
    output.writeUTF(obj)
    output.close()
    fs.close()
  }

  def writeString(obj: String, hdfsUrl: String): Unit = {
    val conf = new Configuration()
    writeString(conf, obj, hdfsUrl)
  }

  lazy val edpTempPrefix = """$$"""
}
