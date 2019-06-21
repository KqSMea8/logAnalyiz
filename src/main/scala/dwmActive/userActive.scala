package dwmActive

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Author wenBin
  * Date 2019/5/6 15:26
  * Version 1.0
  */
object userActive {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)

    val rdd = sc.textFile("E:\\sessionOutputDemo\\2019331.csv")
    import sQLContext.implicits._

    val dataFrame = rdd.map(t => {
      val lines = t.split("\t")
      (lines(0).substring(0, 10), lines(1))
    }).distinct().toDF("logintime", "ip")
    dataFrame.createOrReplaceTempView("tp")


    // 计算dau
    sQLContext.sql(
      """
        |select logintime,count(*) dau from tp group by logintime
      """.stripMargin)

    // 计算wau
    val waudataframe = sQLContext.sql(
      """
        |select
        |			date,
        |	    (select count(distinct a.ip) from tp a where logintime = dau.date) as dau,
        |      (select count(distinct b.ip) from tp b where logintime >= date_sub(dau.date,6)) as wau,
        |      (select count(distinct c.ip) from tp c where logintime >= date_sub(dau.date,29)) as mau
        |from (select distinct logintime date from tp where logintime > '2019-01-01') dau
      """.stripMargin)
    waudataframe.show()


    sc.stop()
  }

}
