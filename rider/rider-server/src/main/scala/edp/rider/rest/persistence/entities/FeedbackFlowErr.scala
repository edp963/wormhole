package edp.rider.rest.persistence.entities

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.Tag

case class FeedbackFlowErr(id: Long,
                       projectId:Long,
                       batchId: String,
                       streamId: Long,
                       flowId: Long,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       dataType: String,
                       errorPattern: String,
                       topics: String,
                       errorCount: Int,
                       errorMaxWaterMarkTs: String,
                       errorMinWaterMarkTs: String,
                       errorInfo: String,
                       dataInfo: String,
                       feedbackTime: String,
                       createTime: String)  extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

class FeedbackFlowErrTable(_tableTag: Tag) extends BaseTable[FeedbackFlowErr](_tableTag,"feedback_flow_error"){
   def * = (id,projectId,batchId,streamId,flowId,sourceNamespace,sinkNamespace,dataType,errorPattern,
          topics,errorCount,errorMaxWaterMarkTs,errorMinWaterMarkTs,errorInfo,dataInfo,feedbackTime,createTime)<> (FeedbackFlowErr.tupled, FeedbackFlowErr.unapply)
  val projectId:Rep[Long]=column[Long]("project_id")
   val batchId:Rep[String]=column[String]("batch_id")
   val streamId:Rep[Long]=column[Long]("stream_id")
   val flowId:Rep[Long]=column[Long]("flow_id")
   val sourceNamespace:Rep[String]=column[String]("source_ns")
   val sinkNamespace: Rep[String]= column[String]("sink_ns")
   val dataType:Rep[String]=column[String]("data_type")
   val errorPattern:Rep[String]=column[String]("error_pattern")
  val topics:Rep[String]=column[String]("topics")
  val errorCount:Rep[Int]=column[Int]("error_count")
  val errorMaxWaterMarkTs:Rep[String]=column[String]("error_max_watermark_ts")
  val errorMinWaterMarkTs:Rep[String]=column[String]("error_min_watermark_ts")
  val errorInfo:Rep[String]=column[String]("error_info")
  val dataInfo:Rep[String]=column[String]("data_info")
  val feedbackTime:Rep[String]=column[String]("feedback_time")
}
