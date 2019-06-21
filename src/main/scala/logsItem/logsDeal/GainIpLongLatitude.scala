package logsItem.logsDeal

import com.maxmind.geoip.LookupService
import dao.IPSeeker
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/6/18 15:10
  * Version 1.0
  * 1、ip to longitude latitude
  * 2、url to page
  * E:/sessionOutputDemo/originallogs/
  * E:/sessionOutputDemo/gainiplonglatitude/
  */
object GainIpLongLatitude {

  def main(args: Array[String]): Unit = {

    // judge input output is not null
    if (args.length != 2) {
      println("Directory is Error!")
      sys.exit()
    }


    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[4]")
    val sparkContext = new SparkContext(conf)

    // 设置log级别
    sparkContext.setLogLevel("WARN")

    val Array(inputPath, outputPath) = args

    val fileRdd = sparkContext.textFile(inputPath)
    fileRdd.persist()

    val ipRdd = fileRdd.map(rdd => {
      val lines = rdd.split("\t")
      lines(1)
    }).distinct()
    //println(ipRdd.count())

    val firstRdd = ipRdd.map(rdd => {

      val ip = rdd
      // ip to longitude latitude
      val cl = new LookupService("E:\\ideaProject\\logAnalyiz\\src\\main\\scala\\filePack\\GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
      val ips = new IPSeeker

      val l2 = cl.getLocation(ip)
      if (l2 != null && !l2.equals("")) {

        val city = l2.city
        val city2 = ips.getAddress(ip)
        val region = l2.region
        val latitude = l2.latitude
        val longitude = l2.longitude
        //println(ip + "\t" + city + "\t" + region + "\t" + latitude + "\t" + longitude)
        ip + "\t" + city + "\t" + city2 + "\t" + region + "\t" + latitude + "\t" + longitude

      } else {
        ""
      }
    }).filter(rdd => !rdd.isEmpty)

    firstRdd.foreach(println)
    println(firstRdd.count())

    //firstRdd.coalesce(1).saveAsTextFile(outputPath)

    sparkContext.stop()
  }
}
