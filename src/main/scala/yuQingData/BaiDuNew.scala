package yuQingData

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/6/4 12:04
  * Version 1.0
  */
object BaiDuNew {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\市场监管总局数据采集\\baidunews.csv")
    // 日期格式转换
    val loc = new Locale("what")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", loc)
    var format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    /**
      * "标题","网址","来源和时间","摘要"
      * 名称 网址 摘要 来源 时间
      * name url abstract source time
      */
    var name = ""
    var url = ""
    var abstracts = ""
    var source = ""
    var time = ""
    //lines.foreach(println)
    val resultRDD = lines.map(rdd => {
      val allLines = rdd.split("\",\"")
      if(allLines.length == 5){

        name = allLines(0).replaceAll(",,,\"", "")
        url = allLines(1)
        val sourceAnddatetime = allLines(4).split("\t")
        source = sourceAnddatetime(0)
        time = sourceAnddatetime(1)
        abstracts = sourceAnddatetime(2)
        name + "\t" + url + "\t" + abstracts + "\t" + source + "\t" + time
      }else{
        println(rdd)
        ""
      }
    }).filter(t => t.length != 0).distinct()

    /**
      * 处理时间
      */
    val baiduresult = resultRDD.map(rdd => {
      val lines = rdd.split("\t")

      //1559637000
      if (lines(4).substring(lines(4).length - 3, lines(4).length) == "分钟前") {

        val longtime: Long = 1559637000
        val minites: Int = lines(4).substring(0, lines(4).length - 3).toInt
        time = format.format((longtime - minites * 60) * 1000)

      } else if (lines(4).substring(lines(4).length - 3, lines(4).length) == "小时前") {
        val longtime: Long = 1559637000
        val hour: Int = lines(4).substring(0, lines(4).length - 3).toInt
        time = format.format((longtime - hour * 60 * 60) * 1000)
      } else {
        time = lines(4)
      }
      lines(0) + "\t" + lines(1) + "\t" + lines(2)+ "\t" + lines(3) + "\t" + time
    })

    baiduresult.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/datePaChong/baidudata")

    sc.stop()
  }

}
