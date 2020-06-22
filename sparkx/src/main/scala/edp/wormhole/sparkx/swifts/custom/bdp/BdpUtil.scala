package edp.wormhole.sparkx.swifts.custom.bdp


import edp.wormhole.util.{MD5Utils, JsonUtils}

/**
  * Created by neo_qiang on 2020/5/5.
  */
object BdpUtil {
  def  getAllUrlFromBdpConfig(dbpconfig:BdpConfig,appidly:String):String={
   dbpconfig.url+""+dbpconfig.biz_id+"/"+dbpconfig.decisionUnit_id+"?"+"app_name="+dbpconfig.app_name+"&apply_id="+appidly+"&app_token="+MD5Utils.getMD5String(dbpconfig.bizToken+appidly)
  }

  def getBdpConfig(dbpStr:String):BdpConfig={
     JsonUtils.json2caseClass[BdpConfig](dbpStr)
  }


}
