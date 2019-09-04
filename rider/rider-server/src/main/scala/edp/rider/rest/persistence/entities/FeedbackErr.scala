package edp.rider.rest.persistence.entities

import edp.rider.rest.persistence.base.{BaseEntity, BaseTable}
import slick.jdbc.MySQLProfile.api._
import slick.lifted.Tag

case class FeedbackErr(id: Long,
                       projectId:Long,
                       batchId: String,
                       streamId: Long,
                       flowId: Long,
                       sourceNamespace: String,
                       sinkNamespace: String,
                       dataType: String,
                       errorPattern: String,
                       topics: Option[String],
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

case class SimpleFeedbackErr(id: Long,
                             projectId:Long,
                             batchId: String,
                             streamId: Long,
                             flowName: String,
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
                             createTime: String)

case class FeedbackErrPartition(`partition_num`:Int,`from_offset`:Long,`until_offset`:Long){
  lazy val num=`partition_num`
  lazy val from=`from_offset`
  lazy val to=`until_offset`
}

case class FeedbackErrTopicInfo(`topic_name`:String,`topic_type`:Option[String],`partition_offset`:Seq[FeedbackErrPartition]){
  lazy val topicName=`topic_name`
  lazy val topicType=`topic_type`.getOrElse("")
  lazy val partitionOffset=`partition_offset`
}

case class RechargeResultLog(id: Long,
                              errorId: Long,
                              detail: String,
                              creator: String,
                              createTime: String,
                              updateTime: String,
                              rst: Int)  extends BaseEntity {
  override def copyWithId(id: Long): this.type = {
    copy(id = id).asInstanceOf[this.type]
  }
}

case class FeedbackPartitionResult(topic: String,
                                   partition: Int,
                                   startTime: String,
                                   endTime: String,
                                   count: Int,
                                   success: Boolean)

class RechargeResultLogTable(_tableTag: Tag) extends BaseTable[RechargeResultLog](_tableTag,"recharge_result_log"){
  def * = (id,errorId,detail,creator,createTime,updateTime,rst) <> (RechargeResultLog.tupled, RechargeResultLog.unapply)
  val errorId:Rep[Long] = column[Long]("error_id")
  val detail:Rep[String] = column[String]("detail")
  val creator:Rep[String] = column[String]("creator")
  val rst:Rep[Int] = column[Int]("rst")
}

class FeedbackErrTable(_tableTag: Tag) extends BaseTable[FeedbackErr](_tableTag,"feedback_error"){
   def * = (id,projectId,batchId,streamId,flowId,sourceNamespace,sinkNamespace,dataType,errorPattern,
          topics,errorCount,errorMaxWaterMarkTs,errorMinWaterMarkTs,errorInfo,dataInfo,feedbackTime,createTime)<> (FeedbackErr.tupled, FeedbackErr.unapply)
  val projectId:Rep[Long]=column[Long]("project_id")
   val batchId:Rep[String]=column[String]("batch_id")
   val streamId:Rep[Long]=column[Long]("stream_id")
   val flowId:Rep[Long]=column[Long]("flow_id")
   val sourceNamespace:Rep[String]=column[String]("source_ns")
   val sinkNamespace: Rep[String]= column[String]("sink_ns")
   val dataType:Rep[String]=column[String]("data_type")
   val errorPattern:Rep[String]=column[String]("error_pattern")
  val topics:Rep[Option[String]]=column[Option[String]]("topics")
  val errorCount:Rep[Int]=column[Int]("error_count")
  val errorMaxWaterMarkTs:Rep[String]=column[String]("error_max_watermark_ts")
  val errorMinWaterMarkTs:Rep[String]=column[String]("error_min_watermark_ts")
  val errorInfo:Rep[String]=column[String]("error_info")
  val dataInfo:Rep[String]=column[String]("data_info")
  val feedbackTime:Rep[String]=column[String]("feedback_time")
}
