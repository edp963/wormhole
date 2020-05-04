package edp.wormhole.sparkx.hdfs

import edp.wormhole.externalclient.hadoop.HdfsUtils
import edp.wormhole.sparkx.spark.log.EdpLogging
import edp.wormhole.sparkxinterface.swifts.WormholeConfig
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

object HdfsFinder extends EdpLogging{
  var hdfsActiveUrl = ""

  def fillWormholeConfig(config: WormholeConfig, hadoopConfig: Configuration)={
      if(config.hdfs_namenode_hosts.nonEmpty)
         config
      else{
         val (nameNodeIds,nameNodeHosts) = getNameNodeInfoFromLocalHadoop(hadoopConfig)
         new WormholeConfig(config.kafka_input,config.kafka_output,config.spark_config,
           config.rdd_partition_number,config.zookeeper_address,config.zookeeper_path,
           config.kafka_persistence_config_isvalid,config.stream_hdfs_address,
           nameNodeHosts,nameNodeIds,config.kerberos,config.hdfslog_server_kerberos, config.special_config)
      }
  }

  private  def getNameNodeInfoFromLocalHadoop(configuration: Configuration)= {
    val nameServiceName = if (configuration.get("dfs.internal.nameservices") != null) configuration.get("dfs.internal.nameservices") else configuration.get("dfs.nameservices")
    if (null == nameServiceName) {
      (None, None)
    } else {
      val nameNodeIds = configuration.get(s"dfs.ha.namenodes.$nameServiceName")
      val nameNodeHosts = nameNodeIds.split(",").map(nodeId => configuration.get(s"dfs.namenode.rpc-address.$nameServiceName.$nodeId")).mkString(",")
      logInfo(s"serviceName:$nameServiceName,nodeIds:$nameNodeIds,nodeHosts:$nameNodeHosts")
      (Some(nameNodeIds), Some(nameNodeHosts))
    }
  }

  def getHadoopConfiguration(config: WormholeConfig): (Configuration,String,String) = {
    val configuration = new Configuration()
    configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
    if (config.kerberos && config.hdfslog_server_kerberos.nonEmpty && !config.hdfslog_server_kerberos.get) {
      configuration.set("ipc.client.fallback-to-simple-auth-allowed", "true")
    }

    val hdfsRoot=if(config.hdfs_namenode_hosts.nonEmpty) findDefaultFs(configuration, config.hdfs_namenode_hosts.get.split(","))
                 else config.stream_hdfs_address.get

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
