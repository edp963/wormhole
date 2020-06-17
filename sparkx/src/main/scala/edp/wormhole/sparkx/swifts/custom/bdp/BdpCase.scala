package edp.wormhole.sparkx.swifts.custom.bdp

/**
  * Created by neo_qiang on 2020/5/5.
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