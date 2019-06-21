package testCompute
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * Author wenBin
  * Date 2019/4/24 11:44
  * Version 1.0
  */
object readOrTable {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val ss = SparkSession.builder().config(conf).getOrCreate()
    val tUserDF: DataFrame = ss.read.format("jdbc").options(Map(
      "url" -> "jdbc:oracle:thin:@47.92.136.8:1521:bioyuan",
      "user" -> "biodev",
      "password" -> "bio#2019Ora",
      "dbtable" -> "T_USER",
      "driver" -> "oracle.jdbc.driver.OracleDriver"
    )).load()
    tUserDF.createOrReplaceTempView("groupData")

    val resultDt: DataFrame = ss.sql("select id,real_name,provincecity_name,region_name from groupData")

    val resultRdd = resultDt.rdd.map(t => {
      // id tea_name province city
      t.getAs[Int](0).formatted("%.0f") + "\t" + t.getAs(1) + "\t" + t.getAs(2) + "\t" + t.getAs(3)
    })

    //resultRdd.foreach(println)

    resultRdd.coalesce(1).saveAsTextFile("E:\\sessionOutputDemo\\teacherinfor")
    sc.stop()
  }
}

