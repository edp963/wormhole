package edp.mad.elasticsearch

case class AppInfos ( madProcessTime: String,
                       appId: String,
                       streamName: String,
                       state: String,
                       finalStatus: String,
                       user: String,
                       queue: String,
                       startedTime: String)

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

case class IndexEntity(
                        index: String,
                        typeName: String,
                        createIndexJson: String,
                        createIndexPattern: String,
                        createIndexInterval:  String, // per days   per month
                        retainIndexDays: String,
                        EsMappingsSchema: Map[String,Any]
                      )

object MadIndex extends Enumeration {
  type MadIndex = Value
  // The infos get by REST API
  val INDEXPROJECTINFOS = Value("mad-project-infos")
  val INDEXSTREAMINFOS = Value("mad-stream-infos")
  val INDEXFLOWINFOS = Value("mad-flow-infos")
  val INDEXAPPINFOS = Value("mad-app-infos")
  val INDEXNAMESPACEINFOS = Value("mad-namespace-infos")

  //  The wormhole feedback message
  val INDEXFLOWFEEDBACK = Value("mad-flow-feedback")
  val INDEXFLOWERROR = Value("mad-flow-error")
  val INDEXSTREAMSFEEDBACK = Value("mad-stream-feedback")
  val INDEXSTREAMERROR = Value("mad-stream-error")

  //  The logs meeesage
  val INDEXAPPLOGS = Value("mad-app-logs")

  // the diagnosis  message
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

