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

import java.sql.Timestamp

import com.alibaba.fastjson.JSON
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.{SwiftsInterface, SwiftsProcessConfig, WormholeConfig}
import edp.wormhole.ums.{UmsOpType, UmsSysField}
import edp.wormhole.util.DateUtils
import org.apache.spark.sql.{DataFrame, SparkSession}

class Snapshot extends SwiftsInterface with EdpLogging {
  override def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String): DataFrame = {
    val tableName = "increment"
    val specialConfig =
      if (config != null && config.specialConfig.nonEmpty && config.specialConfig.getOrElse("") != "") JSON.parseObject(config.specialConfig.get)
      else null
    val startTime = if (specialConfig != null && specialConfig.containsKey("start_time")) specialConfig.getString("start_time").trim else null
    val endTime = if (specialConfig != null && specialConfig.containsKey("end_time")) specialConfig.getString("end_time").trim else null
    val keys = specialConfig.getString("table_keys").trim

    val fromTs = if (startTime == null) DateUtils.dt2timestamp(DateUtils.yyyyMMddHHmmss(DateUtils.unixEpochTimestamp)) else DateUtils.dt2timestamp(startTime)
    val toTs = if (endTime == null) DateUtils.dt2timestamp(DateUtils.currentDateTime) else DateUtils.dt2timestamp(DateUtils.dt2dateTime(endTime))

    df.createOrReplaceTempView(tableName)
    val resultDf = session.sql(getSnapshotSqlByTs(keys, fromTs, toTs, tableName,df.columns.mkString(",")))
    session.sqlContext.dropTempTable(tableName)
    resultDf
  }

  def getSnapshotSqlByTs(keys: String, fromTs: Timestamp, toTs: Timestamp, tableName: String,columns:String): String = {
    s"""
       |select ${columns} from
       |    (select *, row_number() over
       |      (partition by $keys order by ${UmsSysField.ID.toString} desc) as rn
       |    from ${tableName}
       |      where ${UmsSysField.TS.toString} >= '$fromTs' and ${UmsSysField.TS.toString} <= '$toTs')
       |    increment_filter
       |  where ${UmsSysField.OP.toString} != '${UmsOpType.DELETE.toString.toLowerCase}' and ${UmsSysField.OP.toString} != '${UmsOpType.DELETE.toString.toUpperCase()}' and rn = 1
          """.stripMargin.replace("\n", " ")
  }
}
