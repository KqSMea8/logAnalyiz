package yuQingData

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.bson.Document

/**
  * Author wenBin
  * Date 2019/5/22 13:59
  * Version 1.0
  * 读取Mongo-Tweets微博数据抽取有效字段插入到Mongo-tf_news_info_record
  */
object WBReadMongo {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("MyApp")
      //.config("spark.mongodb.input.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_news_info_record?")
      //.config("spark.mongodb.input.uri", "mongodb://spider:spider9102@118.31.44.140:8000/Sina.Tweets?authSource=admin")
      .config("spark.mongodb.input.uri", "mongodb://spider:spider9102@118.31.44.140:8000/Sina.Tweets?")
      .config("spark.mongodb.output.uri", "mongodb://spider:spider9102@118.31.44.140:8000/lyricInfo.tf_news_info_record?")
      .getOrCreate()


    // 设置log级别
    spark.sparkContext.setLogLevel("WARN")

    val df = MongoSpark.load(spark)

    df.createOrReplaceTempView("Tweets")

    //val resDf: DataFrame = spark.sql("select abstractInfo,mediaSources,publishTime,title,url from Tweets where mediaSources in ('人民日报','新华社','光明日报','经济日报','中央广播电台','中央电视台','中国日报','工人日报','中新社','人民网','新华网','中国经济网')")
    val resDf: DataFrame = spark.sql("select * from Tweets where job_id = 'Z2019' and created_at like '2019-05%'")
    //val resDf: DataFrame = spark.sql("select * from Tweets")
    //select context,created_at,weibo_url, from Comments
    // 人民日报、新华社、光明日报、经济日报、中央广播电台、中央电视台、中国日报、工人日报、中新社、人民网、新华网、中国经济网
    // select * from Tweets where mediaSources in ('人民日报','新华社','光明日报','经济日报','中央广播电台','中央电视台','中国日报','工人日报','中新社','人民网','新华网','中国经济网')
    //resDf.show()


    /**
      * _id avatar comment_num content crawl_time
      * created_at image_url job_id like_num location
      * nick_name origin_weibo repost_num tool user_id weibo_url
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
        .append("planId", "5d072d020906b7045c630daf")
        .append("taskId", "5d072d020906b7045c630daf")
        .append("platFormsType", "WB")
        .append("mediaSources", rdd.getString(10))
        .append("title", rdd.getString(10))
        .append("url", rdd.getString(15))
        .append("abstractInfo", rdd.get(3))
        .append("publishTime", rdd.getString(5))
        .append("forwardNumber", rdd.getInt(12))
        .append("commentNumber", rdd.getInt(2))
        .append("likeNumber", rdd.getInt(8))
        .append("createUser", "123")
        .append("isAble", 1)
        .append("isDelete", 0)
      document
    }).filter(t => !t.isEmpty).distinct()




    // resDf.show()
    //println(resultrdd.count())
    //resultrdd.foreach(println)

    // 向mongo里写数据
    MongoSpark.save(resultrdd)
    println(resultrdd.count())

    spark.stop()
    System.exit(0)
  }

}
