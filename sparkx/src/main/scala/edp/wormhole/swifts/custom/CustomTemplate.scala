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


package edp.wormhole.swifts.custom

import edp.wormhole.sparkxinterface.swifts.{SwiftsInterface, SwiftsProcessConfig}
import org.apache.spark.sql._

case class User(id: Int,
                name: String)

class CustomTemplate extends SwiftsInterface {
  override def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig): DataFrame = {
    val rdd = df.rdd.map(row =>
      //TODO 解析数据逻辑
      row.mkString(""))
    session.sqlContext.createDataFrame(rdd, classOf[User])
//   df.withColumn("addColumn",functions.lit(0))
  }
}
