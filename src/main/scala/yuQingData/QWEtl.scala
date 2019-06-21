package yuQingData

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/6/4 12:04
  * Version 1.0
  */
object QWEtl {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\市场监管总局数据采集\\qw\\*")
    // 日期格式转换
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    var format = new SimpleDateFormat("yyyy年MM月dd日 HH:mm")

    /**
      * "标题","网址","来源和时间","来源和时间","摘要"
      * 名称 网址 摘要 来源 时间
      * name url abstract source time
      */
    var title = ""
    var url = ""
    var abstracts = ""
    var source = ""
    var time = ""
    //lines.foreach(println)
    val resultRDD = lines.map(rdd => {
      val allLines = rdd.split("\t")
      if(allLines.length >= 7){

        title = allLines(0)
        url = allLines(1)
        source = allLines(4).replaceAll("\"","").replaceAll("\\?","")
        val times =  allLines(5).replaceAll("\"","")
        //println(times +"      "+url)
        time = fm.format(format.parse(times))
        abstracts = allLines(6).replaceAll("\"","").replaceAll("\\?","")
        title + "\t" + url + "\t" + abstracts + "\t" + source + "\t" + time
      }else{
        //println(rdd)
        ""
      }
    }).filter(t => t.length != 0).distinct()


    //resultRDD.foreach(println)
    //println(resultRDD.count())

    resultRDD.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/datePaChong/qwdata")

    sc.stop()
  }

}
