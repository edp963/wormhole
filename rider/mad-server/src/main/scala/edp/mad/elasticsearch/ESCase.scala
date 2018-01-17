package edp.mad.elasticsearch

/* ----------------------  The Settings information on Rider   ---------------------- */
case class ProjectInfos(    madProcessTime :  String,
                             projectId :  Long,
                             projectName :  String,
                             projectResourceCores :  Int,
                             projectResourceMemory :  Int,
                             projectCreatedTime :  String,
                             projectUpdatedTime :  String
                            )

case class StreamInfos( madProcessTime :  String,
                         // Project 相关配置和静态信息
                         projectId :  Long,
                         projectName :  String,
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
                         executorNums: Int,
                         useCores: Int,
                         useMemoryG: Int,
                         kafkaConnection : String,
                         topicList: String
                       )

case class FlowInfos(    madProcessTime: String,
                           projectId :  Long,
                           projectName :  String,
                           streamId: Long,
                           streamName:  String,
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
                           transPushdownNamespaces: String
                         )

case class AppInfos ( madProcessTime: String,
                       appId: String,
                       streamName: String,
                       state: String,
                       finalStatus: String,
                       user: String,
                       queue: String,
                       startedTime: String)

case class NamespaceInfos ( madProcessTime: String,
                             namespace: String,
                             nsSys: String,
                             nsInstance: String,
                             nsDatabase: String,
                             nsTable: String,
                             topic: String )

case class StreamAlert ( madProcessTime: String,
                            projectId :  Long,
                            projectName :  String,
                            streamId: Long,
                            streamName:  String,
                            streamStatus : String,
                            appId: String,
                            state: String,
                            finalStatus: String,
                            alertLevel: String)

/* ----------------------  The feedback or logs information returned by streams   ---------------------- */
case class StreamFeedback(  madProcessTime :  String,
                             projectId :  Long,
                             projectName :  String,
                             streamId : Long,
                             streamName : String,
                             streamStatTs : String,
                             errorMessage : String,
                             topicName : String,
                             partitionNum :  Int,
                             partitionId :  Int,
                             latestOffset : Long,
                             feedbackOffset : Long
                           )

case class FlowFeedback(    madProcessTime: String,
                            feedbackTs: String,
                            streamId: Long,
                            streamName:  String,
                            flowId: Long,
                            flowNamespace:  String,
                            sourceNamespace:  String,
                            sinkNamespace:  String,
                            topicName: String,
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

case class AppLogs(  madProcessTime: String,
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

  val INDEXPROJECTINFOS = Value("mad-project-infos")
  val INDEXSTREAMINFOS = Value("mad-stream-infos")
  val INDEXFLOWINFOS = Value("mad-flow-infos")
  val INDEXAPPINFOS = Value("mad-app-infos")
  val INDEXNAMESPACEINFOS = Value("mad-namespace-infos")

  val INDEXFLOWFEEDBACK = Value("mad-flow-feedback")
  val INDEXSTREAMSFEEDBACK = Value("mad-stream-feedback")
  val INDEXAPPLOGS = Value("mad-app-logs")

  val INDEXSTREAMALERT = Value("mad-stream-alert")

  def madIndex(s: String ) = MadIndex.withName(s)
}

object MadIndexPattern  extends Enumeration {
  type MadIndexPattern = Value

  val YYYYMMDD = Value("YYYY-MM-DD")
  val YYYYMM = Value("YYYY-MM")
  val NONEPARTITION = Value("")

  def madIndexPattern(s: String ) = MadIndexPattern.withName(s)
}

object MadCreateIndexInterval extends Enumeration {
  type MadCreateIndexInterval = Value

  val NENVER = Value("0")
  val EVERYDAY = Value("1")
  val EVERYMONTH = Value("3")

  def madCreateIndexInterval(s: String) = MadCreateIndexInterval.withName(s)
}

