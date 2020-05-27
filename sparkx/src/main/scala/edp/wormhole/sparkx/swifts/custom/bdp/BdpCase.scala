package edp.wormhole.sparkx.swifts.custom.bdp

/**
  * Created by neo_qiang on 2020/5/5.
  */
/*"http://rule.bdp.creditease.corp/api/credit-v2/decisionUnit/{biz_id}/{decisionUnit_id}?app_name=APP_NAME&apply_id=APPLY_ID&app_token=APP_TOKEN"
  *谛听请求格式
  *
  */
case class BdpConfig(
                    url:String,
                    biz_id:String,
                    decisionUnit_id:String,
                    app_name:String,
                    apply_id:String,
                    app_token:String,
                    bizToken:String,
                    key:String,
                    joinKey:String,
                    allmassage:Seq[Massagepath]
                  )
case  class Massagepath(
                       key:String,
                       path:String,
                       dataType:String,
                       asName:Option[String]=None,
                       nullable:Option[Boolean]=Some(true)
                       )