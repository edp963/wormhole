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

import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{SwiftsProcessConfig, WormholeConfig}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Transform extends EdpLogging {

  def process(session: SparkSession, sourceNs: String, sinkNs: String, inputDf: DataFrame, actions: Array[String], specialConfig: Option[String]): DataFrame = {
    val tmpTable = "increment"
    var currentDf = inputDf
    val tableName = sourceNs.split("\\.")(3)
    actions.foreach { action =>
      val equalMarkPosition = action.indexOf("=")
      val processingType = action.substring(0, equalMarkPosition).trim
      val content = action.substring(equalMarkPosition + 1).trim
      processingType match {
        case "spark_sql" =>
          val mapSql = replaceTable(content, tableName, tmpTable)
          currentDf.createOrReplaceTempView(tmpTable)
          currentDf = session.sql(mapSql)
          session.sqlContext.dropTempTable(tmpTable)
        case "custom_class" =>
          val clazz = Class.forName(content)
          val reflectObject: Any = clazz.newInstance()
          val transformMethod = clazz.getMethod("transform", classOf[SparkSession], classOf[DataFrame], classOf[SwiftsProcessConfig], classOf[WormholeConfig], classOf[String], classOf[String])

          currentDf = transformMethod.invoke(reflectObject, session, currentDf, SwiftsProcessConfig(specialConfig = specialConfig), null, sourceNs, sinkNs).asInstanceOf[DataFrame]
        case _ => logInfo("unsupported processing type, e.g. spark_sql, custom_class.")
      }
    }
    currentDf
  }

  private def replaceTable(sql: String, tableName: String, tmpTable: String): String = {
    val tableNameIndentifyRegex = "(\\s+" + tableName + "\\.\\S+)|(\\s+" + tableName + "(\\s*;|\\s*))|(\\s+" + tableName + "$)"
    tableNameIndentifyRegex.r.replaceSomeIn(sql, (matcher) => {
      Option(matcher.group(0).replaceAll(tableName, tmpTable))
    })
  }

}
