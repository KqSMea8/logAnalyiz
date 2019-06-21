package mongodbanaly

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
  * Author wenBin
  * Date 2019/6/17 17:29
  * Version 1.0
  */
object newsRecordsToTxt {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .config("spark.mongodb.input.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_news_info_record?")
      .getOrCreate()


    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val df = MongoSpark.load(spark)

    df.createOrReplaceTempView("Tweets")

    val resDf: DataFrame = spark.sql("select * from  Tweets")

    val resultRDD = resDf.rdd.map(rdd => {
      rdd.get(0) + "\t" + rdd.get(1) + "\t" + rdd.get(2) + "\t" + rdd.get(3) + "\t" + rdd.get(4) + "\t" +
        rdd.get(5) + "\t" + rdd.get(6) + "\t" + rdd.get(7) + "\t" + rdd.get(8) + "\t" + rdd.get(9) + "\t" +
        rdd.get(10) + "\t" + rdd.get(11) + "\t" + rdd.get(12) + "\t" + rdd.get(13)
    })
    //.foreach(println)
    resultRDD.saveAsTextFile("E:/sessionOutputDemo/datePaChong/tf_infos_words")


    spark.stop()
    System.exit(0)
  }


}
