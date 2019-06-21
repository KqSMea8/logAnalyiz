package readHive

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/5/31 17:04
  * Version 1.0
  */
object ReadHdfs {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName(s"${this.getClass}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("hdfs://bioyuanA:8020/flume/userall/20190528/*")


    rdd.foreach(println)

    println(rdd.count())
    sc.stop()
  }

}
