package logsItem

import com.maxmind.geoip.LookupService
import org.apache.spark.{SparkConf, SparkContext}

/**
  * uv计算：
  *   通过IP 获取城市 经纬度信息
  */
object GainIpMessage {

  def main(args: Array[String]): Unit = {

    // 首先判断目录是否为空
    // E:/sessionOutputDemo/uvcompute/*
    // E:/sessionOutputDemo/ipdistince
    if (args.length != 2) {
      println("目录不正确，退出程序")
      sys.exit()
    }
    //创建一个集合存储输入输出目录
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读文件数据
    val lines = sc.textFile(inputPath)

    //进行过滤，要进行特殊处理
    val rowRDD = lines.map(rdd =>{
      val t = rdd.split("\"")
      val de_te_ip_session = t(0).split(" ")
      var login = ""
      var shebei = ""
      if (1 < t(1).split(" ").length){
        if(3 < t(1).split(" ")(1).split("/").length){
          if (5 <= t(1).split(" ")(1).split("/")(3).length){
            login = t(1).split(" ")(1).split("/")(3).substring(0,5)
          }
          if(4 < t(2).split(" ").length){
            shebei = t(2).split(" ")(4).replace("(","").replace(";","")
          }
          // 2018-08-02 00:17:53 61.163.217.187 - "GET /favicon.ico HTTP/1.0" 302 - Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36
          // dateday datemiao ip session operation equitment
          de_te_ip_session(0)+" "+de_te_ip_session(1)+"\t"+de_te_ip_session(2)+"\t"+de_te_ip_session(3).replace("-","null")+"\t"+ login + "\t" + shebei
        }else {
          ""
        }
      }else{
        ""
      }
    }).filter(t=>t.length != 0)
      .filter(t=> t.split("\t").length > 3 && t.split("\t")(3).equals("login"))
      .distinct()
    //进行过滤，要进行特殊处理
    val resultRDD = rowRDD.map(t => {

      val lines = t.split("\t")
      // 获取位置信息
      val ip = lines(1)
      if (6 < ip.length) {
        ip
      } else {
        ""
      }
    }).filter(t => t.length != 0)
      .distinct()
      .map(t => {
      val ip = t
      // ip解析经纬度
      val cl = new LookupService("E:\\ideaProject\\logAnalyiz\\src\\main\\scala\\filePack\\GeoLiteCity.dat", LookupService.GEOIP_MEMORY_CACHE)
      val l2 = cl.getLocation(ip)
      if (l2 != null && !l2.equals("")) {

        val city = l2.city
        val region = l2.region
        val latitude = l2.latitude
        val longitude = l2.longitude
        println(ip + "\t" + city + "\t" + region + "\t" + latitude + "\t" + longitude)
        ip + "\t" + city + "\t" + region + "\t" + latitude + "\t" + longitude
      } else {
        println("")
        ""
      }
    }).filter(t => t.length != 0).distinct()
    resultRDD.coalesce(1).saveAsTextFile(outputPath)
    sc.stop()
  }

}
