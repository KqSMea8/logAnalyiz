package logsItem

import com.maxmind.geoip.LookupService
import dao.IPSeeker
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Author wenBin
  * Date 2019/5/8 11:48
  * Version 1.0
  */
object UserNameEtl {

  def main(args: Array[String]): Unit = {

    // 首先判断目录是否为空
    //C:/sessionDemo/*
    if (args.length != 1) {
      println("目录不正确，退出程序")
      sys.exit()
    }
    //创建一个集合存储输入输出目录
    val Array(inputPath) = args
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //开始读取数据
    val lines = sc.textFile(inputPath)
    val realUserTable = sc.textFile("C:\\Users\\Administrator\\Desktop\\用户日志\\realUserTable.txt")
    //进行过滤，要进行特殊处理

    var username = "";
    var equitment = "";
    // 清洗数据
    val originalRdd = lines
        .filter(t => t.split(" ").length > 5)
      .filter(t => t.split(" ")(5).length > 40)
      .filter(t => t.split(" ")(5).substring(0, 17) == "/rest/user/login?")
      .filter(t => t.split(" ")(5).split("\\?")(1).substring(0, 8) == "UserName")
      .map(t => {
        val lineArray = t.split(" ", 10)

        if (lineArray(5).split("&")(0).split("=").length == 2) {
          username = lineArray(5).split("&")(0).split("=")(1)
        }

        equitment = lineArray(9).split(" ", 3)(1)
          .replaceAll("\\(", "").replaceAll("\\;", "")

        // 日期 ip username 设备
        //lineArray(0) + " " + lineArray(1) + "\t" + lineArray(2) + "\t" + username + "\t" + equitment
        lineArray(0)  + "\t" + lineArray(2) + "\t" + username + "\t" + equitment
      })
    println(originalRdd.count())

    // 解析IP地址到城市
    val cityRdd = originalRdd.map(t => {
      val lines = t.split("\t")
      val ip = lines(1)
      val is = new IPSeeker
      val regionCity = is.getAddress(ip)
      val region_isp = regionCity.split("\\|")

      if (1 < region_isp.length) {

        var province = region_isp(0) // 省份
        // val city = region_isp(0)    // 城市
        var isp = region_isp(1) // 移动 联通 电信终端

        // 日期 ip username 设备 城市信息 终端
        lines(0) + "\t" + ip + "\t" + lines(2) + "\t" + lines(3) + "\t" + province + "\t" + isp
      }else {
        lines(0) + "\t" + ip + "\t" + lines(2) + "\t" + lines(3) + "\t" + regionCity + "null"
      }
    })
    //cityRdd.foreach(println)
    //println(cityRdd.count())
    //解析ip地址到经纬度
    val resultRdd = cityRdd.map(t => {
      val lines = t.split("\t")
      val ip = lines(1)

      val cl = new LookupService("E:\\ideaProject\\logAnalyiz\\src\\main\\scala\\filePack\\GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
      val l2 = cl.getLocation(ip)
      if (l2 != null && !l2.equals("")) {

        val city = l2.city
        val region = l2.region
        val latitude = l2.latitude
        val longitude = l2.longitude
        lines(0) + "\t" + ip + "\t" + lines(2) + "\t" + lines(3) + "\t" + lines(4) + "\t" + lines(5)  + "\t" + region + "\t" + latitude + "\t" + longitude
      } else {
        println("")
        ""
      }
    }).filter(t => t.length != 0)

    realUserTable.map(t => {
      t.split("\t")
    })


    resultRdd.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/201905")

    sc.stop()
  }
}
