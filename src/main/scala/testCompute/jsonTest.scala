package testCompute

import com.google.gson.{JsonObject, JsonParser}
import scalaj.http.Http

object jsonTest {


  def main(args: Array[String]): Unit = {

    val ip = "120.253.25.105"

    var (region: String, city: String, isp: String) = gainIpTools(ip)
    println(gainIpTools(ip)._1)
  }

  def gainIpTools(ip: String) = {
    val response = Http(s"http://ip.taobao.com/service/getIpInfo.php?ip=${ip}")
      //.timeout(connTimeoutMs = 2000, readTimeoutMs = 5000)
      .asString
      .body

    val jsonObj: JsonObject = new JsonParser().parse(response).getAsJsonObject()//.getAsJsonPrimitive
    var region = ""
    var city = ""
    var isp = ""
    var key = ""
    val dataElement = jsonObj.getAsJsonObject("data").entrySet().iterator()
    while (dataElement.hasNext) {
      val element = dataElement.next()
      key = element.getKey
      if (key.equals("region")) {
        region = element.getValue.toString.replace("\"", "")
      }
      if (key.equals("city")) {
        city = element.getValue.toString.replace("\"", "")
      }
      if (key.equals("isp")) {
        isp = element.getValue.toString.replace("\"", "")
      }
    }
    (region, city, isp)
  }

}
