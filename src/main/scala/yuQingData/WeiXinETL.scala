package yuQingData

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/6/4 12:04
  * Version 1.0
  */
object WeiXinETL {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\市场监管总局数据采集\\weixin.txt")
    // 日期格式转换
    val loc = new Locale("what")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", loc)
    var format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    /**
      * URl,标题,来源,时间,摘要
      * 名称 网址 摘要 来源 时间
      * name url abstract source time
      */
    var name = ""
    var url = ""
    var abstracts = ""
    var source = ""
    var time = ""
    //lines.foreach(println)
    val resultRDD = lines.map(f = rdd => {
      val allLines = rdd.split("\t", 5)

      url = allLines(0)
      name = allLines(1).replaceAll("\"", "")
      source = allLines(2)
      abstracts = allLines(4).replaceAll("\"", "")


      //var abc_soudate: Array[String] = new Array(5)
      val times = allLines(3)
      if (times.length < 7) {

        if (times.substring(times.length - 2, times.length) == "天前") {
          val longtime: Long = 1559637000
          val day: Int = times.substring(0, times.length - 2).toInt
          time = format.format((longtime - day * 60 * 3600) * 1000)
        } else {

          if (times.substring(times.length - 3, times.length) == "分钟前") {

            val longtime: Long = 1559637000
            val minites: Int = times.substring(0, times.length - 3).toInt
            time = format.format((longtime - minites * 60) * 1000)

          } else if (times.substring(times.length - 3, times.length) == "小时前") {
            val longtime: Long = 1559637000
            val hour: Int = times.substring(0, times.length - 3).toInt
            time = format.format((longtime - hour * 60 * 60) * 1000)
          }
        }
      } else if(times.length >= 7){
        time = times.replaceAll("/", "-")
      }else {
        ""
        println("")
      }

      name + "\t" + url + "\t" + abstracts + "\t" + source + "\t" + time
    }).distinct()

    //resultRDD.foreach(println)

    println(resultRDD.count())

    resultRDD.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/datePaChong/weixin")

    sc.stop()
  }

}
