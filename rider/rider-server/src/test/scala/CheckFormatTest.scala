import edp.rider.rest.util.InstanceUtils

object CheckFormatTest extends App {
  val nsSys = "http"
  val url = "http://ip:9000"
  val flag = InstanceUtils.checkFormat(nsSys, url)
  println(flag)
}
