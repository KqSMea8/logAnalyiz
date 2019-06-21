package yuQingData

import java.text.SimpleDateFormat

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
  * Author wenBin
  * Date 2019/6/5 14:45
  * Version 1.0
  */
object StaticRecordMDB {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .config("spark.mongodb.input.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_news_info_record?")
      //.config("spark.mongodb.input.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_daily_record?")
      .config("spark.mongodb.output.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_daily_record?")
      .getOrCreate()


    var format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val formatDay = new SimpleDateFormat("yyyy-MM-dd")

    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val df: DataFrame = MongoSpark.load(spark)
    df.createOrReplaceTempView("tf_news_info_record")

    //df.rdd.foreach(println)

    /**
      * 统计每天tf_daily_record
      */
    var dateDay = ""
    val countResult = df.rdd.map(rdd => {
      dateDay = rdd.getString(11).split(" ")(0)
      //2019年03月27日 09:53
      val timeDay = dateDay.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
      val resultime = formatDay.format(formatDay.parse(timeDay))
      (resultime, 1)
    }).reduceByKey(_ + _).sortBy(_._2,false)
    //countResult.sortBy(_._2,false).foreach(println)
    val value = countResult.map(rdd => {
      if (rdd._1 >= "2019-05-01" && rdd._1 <= "2019-05-31") {
        rdd
      }
    }).filter(rdd => rdd != ())

    val results = value.map(rdd => {
      val rdd1 = rdd.toString.split(",")
      (rdd1(0).replaceAll("\\(", ""), rdd1(1).replaceAll("\\)", ""))
    })
    val dayData: RDD[Document] = results.map(rdd => {
      val dateDay = rdd._1
      val countValues = rdd._2

      val document = new Document()
      //name url abstract source timed
      document
        .append("planId", "5d072d020906b7045c630daf")
        .append("statisticsDate", dateDay)
        .append("newsCount", countValues)
        .append("endTime", "2019-05-31")
        .append("createUser", "123")
        .append("isAble", 1)
        .append("isDelete", 0)

      document
    })
    //dayData.foreach(println)


    // 统计source
    val resSource: DataFrame = spark.sql(
      """
        |select * from
        |     (select
        |           mediaSources,count(*) as values
        |             from tf_news_info_record group by mediaSources) a
        |     order by a.values desc limit 200
      """.stripMargin)
    //resSource.rdd.foreach(println)
    /**
      * 统计来源  tf_media_record
      */
    val sourceData: RDD[Document] = resSource.rdd.map(rdd => {

      val dataSource = rdd.getString(0)
      val countValues = rdd.get(1)

      val document = new Document()
      //name url abstract source timed
      document
        .append("planId", "5cf64aa3282fa415dd86fe83")
        .append("mediaSources", dataSource)
        .append("newsCount", countValues)
        .append("isAble", 1)
        .append("isDelete", 0)

      document
    })

    //sourceData.foreach(println)
    //resSource.show()
    //println(resSource.count())

    //dayData.foreach(println)
    // 向mongo里写数据
    MongoSpark.save(dayData)

    spark.stop()
    System.exit(0)
  }

}
