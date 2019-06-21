package logsItem

import dao.IPSeeker
import org.apache.spark.{SparkConf, SparkContext}
/**
  * 统计计算pv:
  *   通过ip 获取各个省份登陆分布计算
  */
object ipGainLocate {

  def main(args: Array[String]): Unit = {

    // 首先判断目录是否为空
    if (args.length != 2) {
      println("目录不正确，退出程序")
      sys.exit()
    }
    // 创建一个集合存储输入输出目录
    // C:/sessionDemo/*
    // E:/sessionOutputDemo/ip_city
    val Array(inputPath,outputPath) = args
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读文件数据
    val lines = sc.textFile(inputPath)

    //进行过滤，要进行特殊处理
    val rowRDD = lines.map(rdd => {
      val t = rdd.split("\"")
      val de_te_ip_session = t(0).split(" ")
      var login = ""
      var shebei = ""
      if (1 < t(1).split(" ").length) {
        if (3 < t(1).split(" ")(1).split("/").length) {
          if (5 <= t(1).split(" ")(1).split("/")(3).length) {
            login = t(1).split(" ")(1).split("/")(3).substring(0, 5)
          }
          if (4 < t(2).split(" ").length) {
            shebei = t(2).split(" ")(4).replace("(", "").replace(";", "")
          }
          de_te_ip_session(0) + " " + de_te_ip_session(1) + "\t" + de_te_ip_session(2) + "\t" + de_te_ip_session(3).replace("-", "null") + "\t" + login + "\t" + shebei
        } else {
          ""
        }
      } else {
        ""
      }
    }).filter(t => t.length != 0)
      .filter(t => t.split("\t").length > 3 && t.split("\t")(3).equals("login"))
      .distinct()
      .map(t => {

        val lines = t.split("\t")
        // 获取位置信息
        val ip = lines(1)
        if (6 < ip.length) {

          //val information = gainIpTools(ip)
          val is = new IPSeeker
          val regionCity = is.getAddress(ip)
          val region_isp = regionCity.split("\\|")

          if (1 < region_isp.length) {

            var province = region_isp(0) // 省份
            // val city = region_isp(0)    // 城市
            var isp = region_isp(1) // 移动 联通 电信终端

            //截取日期
            val day = lines(0).substring(0, 10)
            if (4 < lines.length) {
              if (province.equals("内蒙古") || province.equals("黑龙江")) {
                day + "\t" + lines(1) + "\t" + lines(2) + "\t" + lines(3) + "\t" + lines(4) + "\t" + province
              } else if (3 < province.length) {
                province = province.substring(0, 2)
                day + "\t" + lines(1) + "\t" + lines(2) + "\t" + lines(3) + "\t" + lines(4) + "\t" + province
              } else {
                // day	ip	session	operation	equitment province
                day + "\t" + lines(1) + "\t" + lines(2) + "\t" + lines(3) + "\t" + lines(4) + "\t" + province
              }
            } else {
              ""
            }
          } else {
            ""
          }
        } else {
          ""
        }
      }).filter(t => t.length != 0).distinct()
    //存储result文件
    //rowRDD.foreach(println)
    rowRDD.coalesce(1).saveAsTextFile(outputPath)
    sc.stop()
  }
}
