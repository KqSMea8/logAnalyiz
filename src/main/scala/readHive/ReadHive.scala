package readHive

import org.apache.spark.sql.SparkSession

/**
  * Author wenBin
  * Date 2019/5/31 14:49
  * Version 1.0
  */
object ReadHive {

  def main(args: Array[String]): Unit = {


    val spark =SparkSession
      .builder()
      .appName("spark-hive")
      .master("local[*]")
      .config("spark.sql.warehouse.dir","hdfs://bioyuanA:8020/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select count(*) from bioyuan.userlogs").show

    spark.stop()
  }

}
