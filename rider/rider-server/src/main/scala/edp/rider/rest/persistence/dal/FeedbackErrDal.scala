package edp.rider.rest.persistence.dal

import edp.rider.module.DbModule.db
import edp.rider.rest.persistence.base.BaseDalImpl
import edp.rider.rest.persistence.entities.{FeedbackErr, FeedbackErrTable}
import edp.rider.rest.util.CommonUtils.{maxTimeOut, minTimeOut}
import slick.lifted.TableQuery

import scala.concurrent.{Await, Future}

class FeedbackErrDal(feedbackErrTable: TableQuery[FeedbackErrTable])
  extends BaseDalImpl[FeedbackErrTable, FeedbackErr](feedbackErrTable){
  def getSinkErrorMaxWatermark(streamId: Long, sourceNs: String, sinkNs: String): Future[Option[String]] = {
    super.findByFilter(str => str.streamId === streamId && str.sourceNamespace === sourceNs && str.sinkNamespace === sinkNs)
      .map[Option[String]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.errorMaxWaterMarkTs).max))
  }

  def getSinkErrorMinWatermark(streamId: Long, sourceNs: String, sinkNs: String): Future[Option[String]] = {
    super.findByFilter(str => str.streamId === streamId && str.sourceNamespace === sourceNs && str.sinkNamespace === sinkNs)
      .map[Option[String]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.errorMinWaterMarkTs).min))
  }

  def getSinkErrorCount(streamId: Long, sourceNs: String, sinkNs: String): Future[Option[Long]] = {
    super.findByFilter(str => str.streamId === streamId && str.sourceNamespace === sourceNs && str.sinkNamespace === sinkNs)
      .map[Option[Long]](seq =>
      if (seq.isEmpty) None
      else Some(seq.map(_.errorCount).sum))
  }

  def deleteHistory(pastNdays: String) = {
    val deleteSeq = Await.result(db.run(feedbackErrTable.withFilter(_.feedbackTime <= pastNdays)
      .map(_.id).result).mapTo[Seq[Long]], minTimeOut)
    if (deleteSeq.nonEmpty) Await.result(super.deleteByFilter(_.id <= deleteSeq.max), maxTimeOut)
  }
}