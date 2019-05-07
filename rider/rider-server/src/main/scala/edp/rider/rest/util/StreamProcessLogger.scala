package edp.rider.rest.util

import java.io.{BufferedInputStream, BufferedReader, InputStream, InputStreamReader}

import edp.rider.common.RiderLogger

class StreamProcessLogger(inputStream: InputStream) extends RiderLogger{
  val streamBuffer = new BufferedReader(new InputStreamReader(new BufferedInputStream(inputStream), "UTF-8"))
  def parseKillErrorStream(): Boolean = {
    var lineNum = 0
    try {
      var line = streamBuffer.readLine
      while (line != null && lineNum < 10) {
//        riderLogger.info(s"read runtime error stream : $line")
        if (line.toLowerCase.contains("Failed to find any Kerberos tgt".toLowerCase)) {
          streamBuffer.close()
          return false
        } else if (line.toLowerCase.contains("Killed application".toLowerCase)) {
          streamBuffer.close()
          return true
        } else {
          line = streamBuffer.readLine
          lineNum += 1
        }
      }
      streamBuffer.close()
      if(lineNum < 10) true
      else false
    } catch {
      case ex: Exception => {
        try{
          streamBuffer.close()
        } catch {
          case exp: Exception => {
            riderLogger.error("streamBuffer close fail: ", exp)
          }
        }
        riderLogger.error("read runtime error stream fail: ", ex)
        false
      }
    }
  }
}
