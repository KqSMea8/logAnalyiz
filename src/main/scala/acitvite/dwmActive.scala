package acitvite

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Author wenBin
  * Date 2019/4/30 10:31
  * Version 1.0
  */
object dwmActive {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val rdd = sc.textFile("E:\\sessionOutputDemo\\2019331uv002.csv")
    import sQLContext.implicits._

    val dataFrame = rdd.map(t => {
      val lines = t.split("\t")
      (lines(0).substring(0, 10), lines(1))
    }).distinct().toDF("logintime", "ip")
    dataFrame.createOrReplaceTempView("tp")

    // 计算day活跃用户数量
    val dauactive = sQLContext.sql(
      """
        |select logintime,count(*) dau from tp group by logintime
      """.stripMargin)
    //dauactive.show()

    // 计算周活跃用户数量
    val wauactive = sQLContext.sql(
      """
        |select date,count(distinct wau) from (
        |select date,
        |       case when BETWEEN DATE_SUB(dau.date,6) AND dau.date then ip end) wau
        |from(select logintime date,ip FROM tp where logintime > '2019-03-01') dau)
      """.stripMargin)
    wauactive.show()

    // 计算月活跃用户数量
    val mauactive = sQLContext.sql(
      """
        |SELECT date,
        |         (SELECT count(distinct ip)  FROM tp WHERE logintime BETWEEN DATE_SUB(dau.date,INTERVAL 29 day)	AND dau.date
        |         ) AS mau
        |FROM ( SELECT logintime AS date,ip FROM tp where logintime > '2019-03-01') dau
      """.stripMargin)
    //mauactive.show()


    sc.stop()
  }

}
