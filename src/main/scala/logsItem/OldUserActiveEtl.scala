package logsItem

import java.text.SimpleDateFormat
import java.util.Locale
import org.apache.spark.{SparkConf, SparkContext}


object OldUserActiveEtl {

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
    val loc = new Locale("what")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", loc)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    //进行过滤，要进行特殊处理
    val rowRDD = lines.map(t => t.split("\"")).map(t => {

      val ip_date_session = t(0).split(" ", 4)
      var login = ""
      var shebei = ""
      //101.199.108.59 - - [29/Jul/2016:19:01:51 +0800] "GET /zhiping/rest/page/login;JSESSIONID=63e3da32-1cff-4671-9604-50cf29993853 HTTP/1.1" 200 3945
      if (ip_date_session.length == 4) {

        if (25 <= ip_date_session(3).length) {

          val dateValue = ip_date_session(3).substring(1, 21)
          val dateValue2 = fm.parse(dateValue).getTime()
          val resultTime = format.format(dateValue2)


          if (1 < t(1).split(" ").length) {
            if (4 < t(1).split(" ")(1).split("/").length) {
              if (5 <= t(1).split(" ")(1).split("/")(4).length) {
                if(!ip_date_session(0).equals("127.0.0.1")){
                  login = t(1).split(" ")(1).split("/")(4).substring(0, 5)
                  // 日期	ip	session	loign	equitment
                  resultTime + "\t" + ip_date_session(0) + "\t" + "null" + "\t" + login + "\t" + "null"
                }else{""}
              }else {""}
            } else {""}
          } else {""}
        } else {""}
      } else {""}
    }).filter(t => t.length != 0)
      .filter(t => t.split("\t").length > 4 && t.split("\t")(3).equals("login"))
      .distinct()

    rowRDD.count()
    //存储result文件
    rowRDD.coalesce(10).saveAsTextFile("E:/sessionOutputDemo/20162018-2")
    sc.stop()
  }
}
