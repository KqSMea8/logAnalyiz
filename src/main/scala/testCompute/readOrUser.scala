package testCompute

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

object readOrUser {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val tUserDF: DataFrame = ss.read.format("jdbc").options(Map(
      "url" -> "jdbc:oracle:thin:@114.55.110.111:1521:orcl",
      "user" -> "zpglxt",
      "password" -> "Bio#888#Zcyd",
      "dbtable" -> "T_LABORATORY",
      "driver" -> "oracle.jdbc.driver.OracleDriver"
    )).load()
    tUserDF.createOrReplaceTempView("groupData")

    val resultDt: DataFrame = ss.sql("select * from groupData")

    val resultRdd = resultDt.rdd

    resultRdd.coalesce(1).saveAsTextFile("E:\\sessionOutputDemo\\laboratory")
    sc.stop()

  }
}
