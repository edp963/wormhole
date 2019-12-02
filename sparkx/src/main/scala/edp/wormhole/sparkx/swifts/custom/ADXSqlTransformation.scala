package edp.wormhole.sparkx.swifts.custom

import edp.wormhole.sparkxinterface.swifts.{SwiftsInterface, SwiftsProcessConfig, WormholeConfig}
import edp.wormhole.util.swifts.SwiftsSql
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{DataFrame, SparkSession}



/**
  * Created by neo_qiang on 2019/5/29.
  */
class ADXSqlTransformation extends SwiftsInterface{
    override def transform(session: SparkSession, df: DataFrame, config: SwiftsProcessConfig, streamConfig: WormholeConfig, sourceNamespace: String, sinkNamespace: String): DataFrame = {
      var temp_df:DataFrame = df
      //Get Schema From Transformation
      val schemasFromSwifts = config.swiftsSql match {
        case Some(array)=> getFieldsfromSwifts(array(1))
        case None => Seq.empty[String]
      }
      //Get Schema From Sprek Stream
      val schemasNameFromStream:Seq[String] = df.columns.toSeq
      val schemasFromStream = schemasNameFromStream.map(schemasName => schemasName.toLowerCase)

      //Scan schemasFromStream To add  Column (type:String , value:null)
      schemasFromSwifts.foreach(schemaName => {
        val schema = schemaName.toLowerCase
        if(config.datasetShow.get) println(s"check Column $schema")
        if(!(schema.equals("ums_uid_")||schema.equals("ums_ts_")||schema.equals("ums_op_")||schema.equals("ums_id_"))&&schemasFromStream.indexOf(schema)<0) {
          if(config.datasetShow.get) println(s"add Column with name : $schema")
          temp_df = temp_df.withColumn(schema,lit(null).cast(StringType))
        }
      })
      temp_df
  }


  def getFieldsfromSwifts(switsSql:SwiftsSql):Seq[String] = {
    val sql = switsSql.sql
    val from_index = sql.toLowerCase().lastIndexOf(" from ")
    val field_str  = sql.trim.substring("select ".length,from_index)
    field_str.split(",").map(f => {
      val index_field_start = f.toLowerCase.lastIndexOf(" as ")
      if(index_field_start<0) f.trim else f.substring(index_field_start+3,f.length).trim
    }).toSeq
  }

}
