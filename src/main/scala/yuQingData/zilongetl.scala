package yuQingData

import java.text.SimpleDateFormat

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/6/4 12:04
  * Version 1.0
  */
object zilongetl {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // 读取数据
    val lines = sc.textFile("C:\\Users\\Administrator\\Desktop\\市场监管总局清洗.csv")
    // 日期格式转换
    val fm = new SimpleDateFormat("yyyy-MM-dd")
    var format = new SimpleDateFormat("yyyy年MM月dd日 HH:mm")

    /**
      * "标题","网址","来源和时间","来源和时间","摘要"
      * 名称 网址 摘要 来源 时间
      * name url abstract source time
      */
    //lines.foreach(println)
    val resultRDD = lines.map(rdd => {
      val lines = rdd.split("\t")

      val strings = lines(0).split("\",\"")

      // 标题 路径 来源 日期 摘要
      strings(0).replaceAll("\"","") +"\t"+ strings(1) +"\t"+ strings(2) +"\t"+ lines(2)  +"\t"+ lines(3)

    }).filter(t => t.length != 0)

    resultRDD.foreach(println)

    println(resultRDD.count())



    resultRDD.coalesce(1).saveAsTextFile("C:\\Users\\Administrator\\Desktop\\市场监管总局结果.txt")

    sc.stop()
  }

}
