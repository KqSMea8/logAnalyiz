package testCompute

import com.maxmind.geoip.LookupService
import dao.{IPEntry, IPSeeker, Utils}

object StringDemo01 {

  def main(args: Array[String]): Unit = {


    val str = "山东省东营市|联通"
    val strings = str.split("\\|")
    //println(strings(0).substring(0,3)+"--"+strings(0).substring(3,6))
    //println(strings(1))

    //val cl = new LookupService("E:\\ideaProject\\logAnalyiz\\src\\main\\scala\\file\\GeoLiteCity-2013-01-18.dat", LookupService.GEOIP_MEMORY_CACHE)
    //val cl = new LookupService("E:\\ideaProject\\logAnalyiz\\src\\main\\scala\\filePack\\GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
    //val l2 = cl.getLocation("194.113.106.126")
    //println("countryCode: " + "==" + l2.countryCode + "\t" + "countryName: " + l2.countryName + "\t" + "region: " + l2.region + "\t" + "city: " + l2.city + "\t" + "latitude: " + l2.latitude + "\t" + "longitude: " + l2.longitude)
    //println(l2)

    val ip = "117.190.233.172"
    val is = new IPSeeker
    val regionCity = is.getAddress(ip)
    val area1 = is.getArea(ip)
    val country = is.getCountry(ip)
    //    println(regionCity)
    //    println(area1)
    //    println(country)

    val cl = new LookupService("E:\\ideaProject\\logAnalyiz\\src\\main\\scala\\filePack\\GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
    val ips = new IPSeeker

    val l2 = cl.getLocation(ip)

    val city = l2.city
    val city2 = ips.getAddress(ip)
    val region = l2.region
    val latitude = l2.latitude
    val longitude = l2.longitude

    println(city+"\t"+city2+"\t"+longitude + "," + latitude)


  }

}
