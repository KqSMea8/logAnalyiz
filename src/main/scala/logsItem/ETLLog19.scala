package logsItem

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}

/**
  * etl log2019
  */
object ETLLog19 {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\用户日志\\log2019\\access.log")
    // 日期格式转换
    val loc = new Locale("what")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", loc)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    var ip = ""
    var date = ""
    var get = ""
    var http = ""
    var equipment = ""
    // 处理数据
    val rowRDD = lines.map(t => {

      val ts = t.split(" ")
      if (18 < ts.length ){

        get = ts(6).replaceAll("\t", "")
        http = ts(10).replaceAll("\"", "")

        if(!get.equals("/") || !http.equals("-")){

            val dateValue = ts(3).substring(1, 21)
            val dateValue2 = fm.parse(dateValue).getTime()
            ip = ts(0)
            date = format.format(dateValue2)

            equipment = ts(12).replaceAll("(","")

            // ip session date http equitment
            ip + "\t" + date + "\t" + get + "\t" + http + "\t" + equipment
        }else{""}
      }else{""}
    }).filter(t => t.length != 0)
      .filter(t => {
      val string = t.split("\t")
      !(string(2)+string(3)).equals("/favicon.ico-")
    })
      .distinct()

    //rowRDD.foreach(println) 2193618
    println(rowRDD.count())
    rowRDD.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/looking")

    sc.stop()
  }

}
