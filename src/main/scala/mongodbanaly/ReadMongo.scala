package mongodbanaly

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document
import org.slf4j.LoggerFactory

/**
  * Author wenBin
  * Date 2019/5/22 13:59
  * Version 1.0
  */
object ReadMongo {



  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      .config("spark.mongodb.input.uri", "mongodb://spider:spider%402019@118.31.44.140:8000/Sina.Comments?authSource=admin")
      //.config("spark.mongodb.input.uri", "mongodb://spider:spider%402019@118.31.44.140:8000/Sina.Tweets?authSource=admin")
      //.config("spark.mongodb.output.uri", "mongodb://spider:spider%402019@118.31.44.140:8000/lyricInfo.tf_news_info_record?authSource=admin")
      //.config("spark.mongodb.input.uri", "mongodb://192.168.8.125:27017/myfirstDB.tf_news_info_record")
      .getOrCreate()


    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val df = MongoSpark.load(spark)

    df.createOrReplaceTempView("Tweets")

    val resDf: DataFrame = spark.sql("select nick_name,created_at,weibo_url,content from Tweets")
    //select context,created_at,weibo_url, from Comments


    /**
      * _id comment_num content crawl_time created_at
      * image_url job_id like_num location origin_weibo repost_num tool user_id weibo_url
      */
    val resultrdd = resDf.rdd.map(rdd => {


      //      rdd.getString(0) +"\t"+
      //        rdd.getInt(1) +"\t"+
      //        rdd.getString(2) +"\t"+
      //        rdd.getInt(3) +"\t"+
      //        rdd.getString(4) +"\t"+
      //        rdd.getString(5)+"\t"+
      //        rdd.getString(6)+"\t"+
      //        rdd.getInt(7)+"\t"+
      //        rdd.getString(8)+"\t"+
      //        rdd.getString(9)+"\t"+
      //        rdd.getInt(10)+"\t"+
      //        rdd.getString(11)+"\t"+
      //        rdd.getString(12)+"\t"+
      //        rdd.getString(13)


      val document = new Document()
      //name url abstract source timed
      document
        .append("planId", "5cf64aa3282fa415dd86fe83")
        .append("taskId", "5cf64aa3282fa415dd86fe83")
        .append("platFormsType", "WB")
        .append("mediaSources", rdd.getString(0))
        .append("title", rdd.getString(0))
        .append("url", rdd.getString(2))
        .append("abstractInfo", rdd.getString(3))
        .append("publishTime", rdd.getString(1))
        .append("forwardNumber", 0)
        .append("commentNumber", 0)
        .append("likeNumber", 0)
        .append("createUser", "123")
        .append("isAble", 1)
        .append("isDelete", 0)
      document
    })




    // resDf.show()
    df.show()
    //println(resDf.count())

    // 向mongo里写数据
    //MongoSpark.save(resultrdd)

    spark.stop()
    System.exit(0)
  }

}
