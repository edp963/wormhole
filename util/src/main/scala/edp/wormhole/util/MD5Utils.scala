package edp.wormhole.util

import java.security.MessageDigest

object MD5Utils {

  def getMD5String(str: String):String = {
    val instance = MessageDigest.getInstance("MD5")
    // 获取MD5算法对象
    val digest = instance.digest(str.getBytes()) // 对字符串加密,返回字节数组

    val sb = new StringBuffer
    digest.foreach(b => {
      val i = b & 0xff
      // 获取字节的低八位有效值
      var hexString = Integer.toHexString(i); // 将整数转为16进制

      if (hexString.length() < 2) {
        hexString = "0" + hexString; // 如果是1位的话,补0
      }
      sb.append(hexString)
    })

    sb.toString
  }
}
