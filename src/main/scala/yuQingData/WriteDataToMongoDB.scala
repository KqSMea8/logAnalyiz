package yuQingData

import com.mongodb.spark.MongoSpark
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.bson.Document

/**
  * 将数据插入到MongoDB
  * Author wenBin
  * Date 2019/6/4 16:59
  * Version 1.0
  */
object WriteDataToMongoDB {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      //.config("spark.mongodb.output.uri", "mongodb://192.168.8.125:27017/myfirstDB.tf_media_record")
      //.config("spark.mongodb.output.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_news_info_record?authSource=admin")
      .config("spark.mongodb.output.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_news_info_record?")
      .getOrCreate()


    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val lines: Dataset[String] = spark.read.textFile("E:\\sessionOutputDemo\\datePaChong\\alldata\\微信.txt")

    var seq: Seq[Document] = Seq()

    /**
      * tf_news_info_record  源数据
      */
    val resultRdd: RDD[Document] = lines.rdd.map(rdd => {

      val lines = rdd.split("\t")
      val document = new Document()
      //title url abstract source time
      // name abstract source time
      if (lines.length >= 4) {
        document
          .append("planId", "5d072d020906b7045c630daf")
          .append("taskId", "5d072d020906b7045c630daf")
          .append("platFormsType", "WX")
          .append("mediaSources", lines(2))
          .append("title", lines(0))
          .append("url", "https://weixin.sogou.com/weixin?type=2&s_from=input&query=%E5%B8%82%E5%9C%BA%E7%9B%91%E7%AE%A1%E6%80%BB%E5%B1%80&ie=utf8&_sug_=n&_sug_type_=")
          .append("abstractInfo", lines(1))
          .append("publishTime", lines(3))
          .append("forwardNumber", 0)
          .append("commentNumber", 0)
          .append("likeNumber", 0)
          .append("createUser", "123")
          .append("isAble", 1)
          .append("isDelete", 0)
      }
      document
    }).filter(t => !t.isEmpty)
    //println(lines.count())

    /**
      * 统计每天tf_daily_record
      */
    val countResult = lines.rdd.map(rdd => {
      val rdds = rdd.split("\t")
      //2019年03月27日 09:53
      val dateDay = rdds(4).split(" ")(0)
      val timeDay = dateDay.replace("年", "-")
        .replace("月", "-")
        .replace("日", "")
      //rdds(0) + "\t" + rdds(1) + "\t" + rdds(2) + "\t" + rdds(3) + "\t" + timeDay
      (timeDay, 1)
    })

    val dayData: RDD[Document] = countResult.reduceByKey(_ + _).map(rdd => {

      val dateDay = rdd._1
      val countValues = rdd._2

      val document = new Document()
      //name url abstract source timed
      document
        .append("planId", "5cf64aa3282fa415dd86fe83")
        .append("statisticsDate", dateDay)
        .append("newsCount", countValues)
        .append("endTime", "2019-06-04")
        .append("createUser", "123")
        .append("isAble", 1)
        .append("isDelete", 0)

      document
    })

    /**
      * 统计来源  tf_media_record
      */
    val SoueceResult = lines.rdd.map(rdd => {
      val rdds = rdd.split("\t")
      //2019年03月27日 09:53
      val sourceDay = rdds(3)

      //rdds(0) + "\t" + rdds(1) + "\t" + rdds(2) + "\t" + rdds(3) + "\t" + timeDay
      (sourceDay, 1)
    })

    val sourceData: RDD[Document] = SoueceResult.reduceByKey(_ + _).map(rdd => {

      val dataSource = rdd._1
      val countValues = rdd._2

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
    //println(sourceData.count())

    resultRdd.foreach(println)
    println(resultRdd.count())
    // 将数据写入mongo
    MongoSpark.save(resultRdd)
    //MongoSpark.save(dayData)
    //MongoSpark.save(sourceData)

    spark.stop()
    System.exit(0)
  }
}





