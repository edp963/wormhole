import edp.wormhole.externalclient.hadoop.HdfsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path


object HdfsUtilsTest extends App {
  val obj = "[{\"encoded\":false,\"name\":\"ums_id_\",\"nullable\":false,\"type\":\"long\"},{\"encoded\":false,\"name\":\"ums_ts_\",\"nullable\":false,\"type\":\"datetime\"},{\"encoded\":false,\"name\":\"ums_op_\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"ums_uid_\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"id\",\"nullable\":false,\"type\":\"long\"},{\"encoded\":false,\"name\":\"client_id\",\"nullable\":false,\"type\":\"long\"},{\"encoded\":false,\"name\":\"order_id\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"ecif_id\",\"nullable\":false,\"type\":\"long\"},{\"encoded\":false,\"name\":\"preaccount_state\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"open_account_result\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"court_dishonest_result\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"credit_result\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"credit_query_count\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"fail_remark\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"pass_remark\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"bank_account_statement_existed\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"mobile_statement_existed\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"credit_report_existed\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"jf_apply_id\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"created_time\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"updated_time\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"deleted_flag\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"app_product\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"app_loan_amount\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"app_call_category\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"offline_info\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"accumulation_fund_check_result\",\"nullable\":false,\"type\":\"string\"},{\"encoded\":false,\"name\":\"insurance_check_result\",\"nullable\":false,\"type\":\"string\"}]\n"
  val hdfsUrl = "hdfs://test/wormhole/hdfslog/schema"
  val configuration = new Configuration(false)
  configuration.addResource(new Path(s"/Users/lwl/Desktop/core-site.xml"))
  configuration.addResource(new Path(s"/Users/lwl/Desktop/hdfs-site.xml"))
  configuration.setBoolean("fs.hdfs.impl.disable.cache", true)
  //configuration.set("fs.defaultFS", "test")

  HdfsUtils.writeString(configuration, obj, hdfsUrl)
  println("end")
}
