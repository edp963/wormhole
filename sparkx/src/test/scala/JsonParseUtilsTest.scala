import edp.wormhole.common.json.{JsonParseUtils, JsonSourceConf, RegularJsonSchema}

object JsonParseUtilsTest extends App {
  val dataParseStr = "{\"fields\":[{\"name\":\"time\",\"type\":\"datetime\",\"nullable\":true},{\"name\":\"time\",\"type\":\"datetime\",\"nullable\":true,\"rename\":\"ums_ts_\"},{\"name\":\"id\",\"type\":\"long\",\"nullable\":true},{\"name\":\"id\",\"type\":\"long\",\"nullable\":true,\"rename\":\"ums_id_\"},{\"name\":\"op\",\"type\":\"string\",\"nullable\":true},{\"nullable\":true,\"name\":\"op\",\"rename\":\"ums_op_\",\"ums_sys_mapping\":\"i:ins,u:upd,d:del\",\"type\":\"string\"},{\"name\":\"name\",\"type\":\"string\",\"nullable\":true},{\"name\":\"before\",\"type\":\"jsonobject\",\"nullable\":true,\"sub_fields\":[{\"name\":\"manager\",\"type\":\"string\",\"nullable\":true,\"rename\":\"before_manager\"},{\"name\":\"mode\",\"type\":\"string\",\"nullable\":true,\"rename\":\"before_mode\"}]},{\"name\":\"after\",\"type\":\"jsonobject\",\"nullable\":true,\"sub_fields\":[{\"name\":\"manager\",\"type\":\"string\",\"nullable\":true,\"rename\":\"after_manager\"},{\"name\":\"mode\",\"type\":\"string\",\"nullable\":true,\"rename\":\"after_mode\"}]}]}"
  val jsonValue1 = "{\"time\":\"2017-12-2210:00:00\",\"id\":1,\"op\":\"ins\",\"name\":null,\"before\":{\"manager\":\"\",\"mode\":\"\"},\"after\":{\"manager\":null,\"mode\":null}}"
  val jsonValue2 = "{\"time\":\"2017-12-2210:00:00\",\"id\":1,\"op\":\"ins\",\"name\":null,\"before\":{\"manager\":\"\",\"mode\":\"\"}}"
  val parseResult: RegularJsonSchema = JsonSourceConf.parse(dataParseStr)
  val result = JsonParseUtils.dataParse(jsonValue2, parseResult.fieldsInfo, parseResult.twoFieldsArr)
  println(result)
}
