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


package edp.wormhole.sparkx.batchjob.transform

import java.util.UUID

import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.SwiftsProcessConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object Transform extends EdpLogging {
  def process(session: SparkSession, inputDf: DataFrame, actions: Array[String], specialConfig: Option[String]): DataFrame = {
    val tableName = "increment"
    //inputDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
    var currentDf = inputDf
    //    var cacheDf = inputDf
    //    var firstInLoop = true

    actions.foreach { case action =>
      val equalMarkPosition = action.indexOf("=")
      val processingType = action.substring(0, equalMarkPosition).trim
      val content = action.substring(equalMarkPosition + 1).trim
      processingType match {
        case "spark_sql" =>
          currentDf.createOrReplaceTempView(tableName)
          currentDf = session.sql(content)
          session.sqlContext.dropTempTable(tableName)
        case "custom_class" =>
          val clazz = Class.forName(content)
          val reflectObject: Any = clazz.newInstance()
          val transformMethod = clazz.getMethod("transform", classOf[SparkSession], classOf[DataFrame], classOf[SwiftsProcessConfig])

          currentDf = transformMethod.invoke(reflectObject, session, currentDf, SwiftsProcessConfig(specialConfig = specialConfig)).asInstanceOf[DataFrame]
        case _ => logInfo("unsupported processing type, e.g. spark_sql, custom_class.")
      }
      //      currentDf.persist(StorageLevel.MEMORY_AND_DISK_SER)
      //      logInfo("currentDf.count:" + currentDf.count())
      //      if (firstInLoop) firstInLoop = false else cacheDf.unpersist()
      //      cacheDf = currentDf
    }
    //    cacheDf.unpersist()
    currentDf
  }
}
