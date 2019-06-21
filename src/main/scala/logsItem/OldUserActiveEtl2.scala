package logsItem

import com.maxmind.geoip.LookupService
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 数据归一化处理
 */
object OldUserActiveEtl2 {

  def main(args: Array[String]): Unit = {

    // 首先判断目录是否为空
    if (args.length != 1) {
      println("目录不正确，退出程序")
      sys.exit()
    }
    //创建一个集合存储输入输出目录
    val Array(inputPath) = args
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    //开始读取数据
    val lines = sc.textFile(inputPath)

    val ipRDD = lines.map(t => {
      val values = t.split("\t")
      val ip = values(1)
      ip
    }).distinct()

    val rowRDD = ipRDD.map(rdd => {
      // ip解析经纬度
      val cl = new LookupService("E:\\ideaProject\\logAnalyiz\\src\\main\\scala\\filePack\\GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
      val l2 = cl.getLocation(rdd)
      if (l2 != null && !l2.equals("")) {

        val city = l2.city
        val region = l2.region
        val latitude = l2.latitude
        val longitude = l2.longitude
        //println(rdd + "\t" + city + "\t" + region + "\t" + latitude + "\t" + longitude)
        rdd + "\t" + city + "\t" + region + "\t" + latitude + "\t" + longitude
      } else {
        println("")
        ""
      }
    }).filter(t => t.length != 0)

    println(rowRDD.count())
    rowRDD.coalesce(1).saveAsTextFile("E:\\sessionOutputDemo\\ip_city")

    sc.stop()
  }

}
