package edp.mad.elasticsearch

case class EsMadFlows(
                       madProcessTime: String,
                       feedbackTs: String,
                       projectId: Long,
                       projectName:  String,
                       // Stream 相关配置和静态信息
                       streamId: Long,
                       streamName:  String,
                       sparkAppId:  String,
                       streamStatus:  String,
                       streamStartedTime:  String,
                       streamConsumerDuration:   Int,
                       streamConsumerMaxRecords:   Int,
                       streamProcessRepartition:   Int,
                       streamDriverCores:   Int,
                       streamDriverMemory:   Int,
                       streamPerExecuterCores:   Int,
                       streamPerExecuterMemory:   Int,
                       kafkaConnection:  String,

                       // Flow 相关配置和静态信息
                       topicName:  String,
                       flowId: Long,
                       flowNamespace:  String,
                       sourceNamespace:  String,
                       sourceDataSystem:  String,
                       sourceInstance:  String,
                       sourceDatabase:  String,
                       sourceTable:  String,
                       sinkNamespace:  String,
                       sinkDataSystem:  String,
                       sinkInstance:  String,
                       sinkDatabase:  String,
                       sinkTable:  String,
                       flowStatus:  String,
                       flowStartedTime:  String,
                       flowUpdateTime:  String,
                       consumedProtocol:  String,
                       sinkSpecificConfig:  String, //每种sink都不一样，无法拆分出有效字段
                       tranConfig:  String,
                       tranActionCustomClass:  String,
                       transPushdownNamespaces: String,

                       // Flow反馈的错误信息
                       flowErrorMaxWaterMarkTs:  String,
                       flowErrorMinWaterMarkTs:  String,
                       flowErrorCount:   Int,
                       flowErrorMessage:  String,
                       // Flow反馈的统计信息
                       statsId:  String,
                       rddCount: Long,
                       throughput: Long,
                       dataOriginalTs: String,
                       rddTransformStartTs:  String,
                       directiveProcessStartTs:  String,
                       mainProcessStartTs:  String,
                       swiftsProcessStartTs:  String,
                       sinkWriteStartTs:  String,
                       processDoneTs:  String,

                       intervalMainProcessToDataOriginalTs: Long,
                       intervalMainProcessToDone: Long,
                       intervalMainProcessToSwifts: Long,
                       intervalMainProcessToSink: Long,
                       intervalSwiftsToSink: Long,
                       intervalSinkToDone: Long,
                       intervalRddToDone: Long
                     )

case class EsMadStreams(
                         madProcessTime :  String,
                         // Project 相关配置和静态信息
                         projectId :  Long,
                         projectName :  String,
                         projectResourceCores :  Int,
                         projectResourceMemory :  Int,
                         projectCreatedTime :  String,
                         projectUpdatedTime :  String,
                         // Stream 相关配置和静态信息
                         streamId : Long,
                         streamName : String,
                         sparkAppId : String,
                         streamStatus : String,
                         streamStartedTime : String,
                         sparkConfig :String,
                         streamConsumerDuration :  Int,
                         streamConsumerMaxRecords :  Int,
                         streamProcessRepartition :  Int,
                         streamDriverCores :  Int,
                         streamDriverMemory :  Int,
                         streamPerExecuterCores :  Int,
                         streamPerExecuterMemory :  Int,
                         kafkaConnection : String,
                         streamStatTs : String,
                         errorMessage : String,
                         topicName : String,
                         partitionNum :  Int,
                         partitionId :  Int,
                         latestOffset : Long,
                         feedbackOffset : Long
                       )

case class EsMadLogs(
                      projectId: Long,
                      projectName: String,
                      streamId: Long,
                      streamName: String,
                      streamStatus: String,
                      logsOrder: String,
                      appId: String,
                      host: String,
                      logTime: String,
                      logstashTime: String,
                      madTime: String,
                      loglevel: String,
                      path: String,
                      className: String,
                      container: String,
                      message: String
                    )

case class IndexEntity(
                        index: String,
                        typeName: String,
                        createIndexJson: String,
                        createIndexPattern: String,
                        createIndexInterval:  String, // per days   per month
                        retainIndexDays: String
                      )

object MadIndex extends Enumeration {
  type MadIndex = Value

  val INDEXFLOWS = Value("mad-flows")
  val INDEXSTREAMS = Value("mad-streams")
  val INDEXLOGS = Value("mad-logs")

  def madIndex(s: String ) = MadIndex.withName(s)
}

object MadIndexPattern  extends Enumeration {
  type MadIndexPattern = Value

  val YYYYMMDD = Value("YYYY-MM-DD")
  val YYYYMM = Value("YYYY-MM")

  def madIndexPattern(s: String ) = MadIndexPattern.withName(s)
}

object MadCreateIndexInterval extends Enumeration {
  type MadCreateIndexInterval = Value

  val EVERYDAY = Value("1")
  val EVERYMONTH = Value("3")

  def madCreateIndexInterval(s: String) = MadCreateIndexInterval.withName(s)
}

