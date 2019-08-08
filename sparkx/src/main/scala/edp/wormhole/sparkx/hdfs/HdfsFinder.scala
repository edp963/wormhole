package edp.wormhole.sparkx.hdfs

import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.sparkx.common.WormholeConfig
import edp.wormhole.sparkx.spark.log.EdpLogging
import org.apache.hadoop.conf.Configuration

object HdfsFinder extends EdpLogging{
  var hdfsActiveUrl = ""
  def getHadoopConfiguration(config: WormholeConfig): (Configuration,String,String) = {
    val configuration = new Configuration()
    configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
    if (config.kerberos && config.hdfslog_server_kerberos.nonEmpty && !config.hdfslog_server_kerberos.get) {
      configuration.set("ipc.client.fallback-to-simple-auth-allowed", "true")
    }
    val hdfsRoot=findDefaultFs(configuration,config.hdfs_namenode_hosts.get.split(","))
    val hdfsTuple=findActiveStreamHdfsAddress(config,hdfsRoot)
    configuration.set("fs.defaultFS",hdfsRoot)

    (configuration,hdfsTuple._1,hdfsTuple._2)
  }

  def getHdfsRelativeFileName(fileName : String)={
      if(fileName==null||fileName.trim.equals(""))
         fileName
      else{
        val hdfsRoot = getHdfsRootFromHdfsPath(fileName)
        fileName.replaceFirst(hdfsRoot,"")
      }
  }

  private def findActiveStreamHdfsAddress(config: WormholeConfig, hdfsActiveRoot: String)={
    val hdfsPath = config.stream_hdfs_address.get
    val hdfsRoot= getHdfsRootFromHdfsPath(hdfsPath)
    if(hdfsRoot.equals(s"hdfs://$hdfsActiveRoot"))
      (hdfsRoot,hdfsPath)
    else
      (s"hdfs://$hdfsActiveRoot",hdfsPath.replaceFirst(hdfsRoot,s"hdfs://$hdfsActiveRoot"))
  }

  def getHdfsRootFromHdfsPath(hdfsPath: String)={
    val hdfsPathGrp = hdfsPath.split("//")
    if (hdfsPathGrp(1).contains("/"))
      hdfsPathGrp(0) + "//" + hdfsPathGrp(1).substring(0, hdfsPathGrp(1).indexOf("/"))
    else hdfsPathGrp(0) + "//" + hdfsPathGrp(1)
  }

  private def findActiveFileName(hdfsFileName: String, hdfsActiveRoot: String)={
    if( hdfsFileName==null || hdfsFileName.startsWith(hdfsActiveRoot))
      hdfsFileName
    else{
      hdfsFileName.replaceFirst(getHdfsRootFromHdfsPath(hdfsFileName), hdfsActiveRoot)
    }
  }

  private def findDefaultFs(configuration: Configuration, hdfsUrls: Array[String]): String ={
    logInfo(s"hdfsActiveUrl before:$hdfsActiveUrl")
    if(hdfsActiveUrl.trim.equals("") || !findActiveHdfsUrl(configuration,hdfsActiveUrl))
      hdfsActiveUrl=hdfsUrls.filterNot(_==hdfsActiveUrl).filter(findActiveHdfsUrl(configuration,_)).take(1).mkString("")
    logInfo(s"hdfsActiveUrl after:$hdfsActiveUrl")
    hdfsActiveUrl
  }

  private def findActiveHdfsUrl(configuration: Configuration, hdfsUrl:String)={
    //test判断查主hdfs namenode方法
    var isHdfsUrlActive=true
    try{
      configuration.set("fs.defaultFS",s"hdfs://$hdfsUrl")
      HdfsUtils.isPathExist(configuration,"/")
    }catch {
      case ex:Throwable =>
        isHdfsUrlActive=false
    }
    isHdfsUrlActive
  }
}
