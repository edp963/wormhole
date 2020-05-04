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


package edp.wormhole.sparkx.swifts.transform

import edp.wormhole.sparkx.common.SparkxUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.swifts.SwiftsConstants
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructField

object SqlBinding extends EdpLogging {

  private[transform] def getCassandraSql(session: SparkSession, tmpLastDf: DataFrame, sourceTableFields: Array[String], lookupTableFields: Array[String], sql: String): String = {
    if(lookupTableFields.nonEmpty) {
      val sourceJoinFieldsContent: Array[String] = getJoinFieldsContent(session, tmpLastDf, sourceTableFields, lookupTableFields)
      sourceJoinFieldsContent.sliding(1000, 1000).map(joinFields => {
        if (lookupTableFields.length == 1) sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields(0) + " in (" + joinFields.mkString(",") + ")")
        else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, "(" + lookupTableFields.mkString(",") + ") in (" + joinFields.mkString(",") + ")")
      }).mkString(" union ") //TODO if sourceJoinFieldsContent is empty
    } else sql
  }

  def getCassandraSqlSingleField(sourceJoinFieldsContent: Set[String],lookupTableFields: String, sql: String): String = {
//    val finalSql= if (!sql.toLowerCase().contains("allow filtering")) {
//       sql + " allow filtering"
//    }
//    else sql
//    sourceJoinFieldsContent.sliding(1000, 1000).map(joinFields => {
//      if (lookupTableFields.length == 1)
        sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields + " in (" + sourceJoinFieldsContent.mkString(",") + ")")
//      else  sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, "(" + lookupTableFields.mkString(",") + ") in (" + sourceJoinFieldsContent.mkString(",") + ")")
//    }).mkString(" union ")  //TODO if sourceJoinFieldsContent is empty
  }

  def getCassandraSqlMutilField(sourceJoinFieldsContent: String,lookupTableFields: Array[String], sql: String): String = {
    //lookupTableFields.length > 1
    var replaceStr = ""
    val content = sourceJoinFieldsContent.trim.substring(1,sourceJoinFieldsContent.length-1).split(",")
    for(i<- lookupTableFields.indices){
      if(i==0) replaceStr = lookupTableFields(i)+"="+content(i)
      else replaceStr = replaceStr + " and " + lookupTableFields(i)+"="+content(i)
    }

    sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, replaceStr)
  }

   def getMysqlSql(sourceJoinFieldsContent: Set[String],lookupTableFields: Array[String], sql: String): String = {
     if(lookupTableFields.nonEmpty) {
       if (sourceJoinFieldsContent.nonEmpty) {
         if (lookupTableFields.length == 1) sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields(0) + " in (" + sourceJoinFieldsContent.mkString(",") + ")")
         else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, "(" + lookupTableFields.mkString(",") + ") in (" + sourceJoinFieldsContent.mkString(",") + ")")
       } else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, " 1=2 ")
     } else sql
  }

   def getSlidingUnionSql(sourceJoinFieldsContent: Set[String], lookupTableFields: Array[String], sql: String): String = {
     if(lookupTableFields.nonEmpty) {
       if (sourceJoinFieldsContent.nonEmpty) {
         sourceJoinFieldsContent.sliding(1000, 1000).map(joinFields => {
           if (lookupTableFields.length == 1) sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields(0) + " in (" + joinFields.mkString(",") + ")")
           else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, "(" + lookupTableFields.mkString(",") + ") in (" + joinFields.mkString(",") + ")")
         }).mkString(" union ")
       } else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, " 1=2 ")
     } else sql
   }

  private[transform] def getSlidingUnionSql(session: SparkSession, tmpLastDf: DataFrame, sourceTableFields: Array[String], lookupTableFields: Array[String], sql: String): String = {
    if(lookupTableFields.nonEmpty) {
      val sourceJoinFieldsContent: Array[String] = getJoinFieldsContent(session, tmpLastDf, sourceTableFields, lookupTableFields)
      if (sourceJoinFieldsContent.nonEmpty) {
        sourceJoinFieldsContent.sliding(1000, 1000).map(joinFields => {
          if (lookupTableFields.length == 1) sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields(0) + " in (" + joinFields.mkString(",") + ")")
          else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, "(" + lookupTableFields.mkString(",") + ") in (" + joinFields.mkString(",") + ")")
        }).mkString(" union ")
      } else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, " 1=2 ")
    } else sql
    //      if (lookupTableFields.length == 1) sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, lookupTableFields(0) + " in ( null ) ")
    //      else sql.replace(SwiftsConstants.REPLACE_STRING_INSQL, "(" + lookupTableFields.mkString(",") + ") in ((" + lookupTableFields.map(_ => "null").mkString(",") + "))")

  }


//   def getFieldContentByType(row: Row, schema: Array[StructField], i: Int): Any = {
//    if (schema(i).dataType.toString.equals("StringType")) {
//      //if (row.get(i) == null) "''"  // join fields cannot be null
//      if (row.get(i) == null) null
//      else "'" + row.get(i) + "'"
//    } else row.get(i)
//  }

  private def getJoinFieldsContent(session: SparkSession, tmpLastDf: DataFrame, sourceTableFields: Array[String], lookupTableFields: Array[String]): Array[String] = {
    import session.implicits._
    tmpLastDf.map(row => {
      val schema: Array[StructField] = row.schema.fields
      val lookupFieldsLength = lookupTableFields.length
      val fieldContent = sourceTableFields.map(fieldName => {
        val index = row.fieldIndex(fieldName)
        SparkxUtils.getFieldContentByTypeForSql(row, schema, index)
      }).mkString(",")
      if (lookupFieldsLength == 1) fieldContent
      else "(" + fieldContent + ")"
    }).collect()
  }
}
