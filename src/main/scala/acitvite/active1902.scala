package acitvite

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/4/25 14:16
  * Version 1.0
  */
object active1902 {

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

    val todayDB = "2019-02-28"
    // 228-->257 225-->130
    // 最近3日留存率计算
    val thrDB = sQLContext.sql(
      s"""
         |select logintime,count(user_id_d1)/count(ip) retention_d1,
         |count(user_id_d3)/count(ip) retention_d3,
         |count(user_id_d5)/count(ip) retention_d5,
         |count(user_id_d7)/count(ip) retention_d7 from(
         |--匹配后续
         |select a.logintime,a.ip,b.ip user_id_d1,c.ip user_id_d3,
         |d.ip user_id_d5,e.ip user_id_d7 from
         |(select logintime,ip from tp a where logintime='${todayDB}')a
         |left join (select * from tp where logintime=date_sub('${todayDB}', 1)) b on a.ip=b.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 3)) c on a.ip=c.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 5)) d on a.ip=d.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 7)) e on a.ip=e.ip)
         |group by logintime
         |union
         |select logintime,count(user_id_d1)/count(ip) retention_d1,
         |count(user_id_d3)/count(ip) retention_d3,
         |count(user_id_d5)/count(ip) retention_d5,
         |count(user_id_d7)/count(ip) retention_d7 from(
         |--匹配后续
         |select a.logintime,a.ip,b.ip user_id_d1,c.ip user_id_d3,
         |d.ip user_id_d5,e.ip user_id_d7 from
         |(select logintime,ip from tp a where logintime=date_sub('${todayDB}', 1))a
         |left join (select * from tp where logintime=date_sub('${todayDB}', 2)) b on a.ip=b.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 4)) c on a.ip=c.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 6)) d on a.ip=d.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 8)) e on a.ip=e.ip)
         |group by logintime
         |union
         |select logintime,count(user_id_d1)/count(ip) retention_d1,
         |count(user_id_d3)/count(ip) retention_d3,
         |count(user_id_d5)/count(ip) retention_d5,
         |count(user_id_d7)/count(ip) retention_d7 from(
         |--匹配后续
         |select a.logintime,a.ip,b.ip user_id_d1,c.ip user_id_d3,
         |d.ip user_id_d5,e.ip user_id_d7 from
         |(select logintime,ip from tp a where logintime=date_sub('${todayDB}', 2))a
         |left join (select * from tp where logintime=date_sub('${todayDB}', 3)) b on a.ip=b.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 5)) c on a.ip=c.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 7)) d on a.ip=d.ip
         |left join (select * from tp where logintime=date_sub('${todayDB}', 9)) e on a.ip=e.ip)
         |group by logintime
      """.stripMargin)

    // 指定日活跃日1日前计算
    val frame = sQLContext.sql(
      s"""
         |select logintime,count(user_id_d1),count(ip)
         |from(
         |--匹配后续
         |select a.logintime,a.ip,b.ip user_id_d1
         |from
         |(select logintime,ip from tp a where logintime=date_sub('${todayDB}', 1))a
         |left join (select * from tp where logintime='${todayDB}') b on a.ip=b.ip
         |)
         |group by logintime
       """.stripMargin)

    // 留存数计算
    val saveDay = sQLContext.sql(
      s"""
         |SELECT
         |    first_day,
         |    sum(case when by_day = 0 then 1 else 0 end) day_0,
         |    sum(case when by_day = 1 then 1 else 0 end) day_1,
         |    sum(case when by_day = 2 then 1 else 0 end) day_2,
         |    sum(case when by_day = 3 then 1 else 0 end) day_3,
         |    sum(case when by_day = 4 then 1 else 0 end) day_4,
         |    sum(case when by_day = 5 then 1 else 0 end) day_5,
         |    sum(case when by_day = 6 then 1 else 0 end) day_6,
         |    sum(case when by_day >= 7 then 1 else 0 end) day_7plus
         |FROM
         |   (SELECT
         |      ip,
         |      logintime,
         |      first_day,
         |      DATEDIFF(logintime,first_day) as by_day
         |   FROM
         |     (SELECT
         |        b.ip,
         |        b.logintime,
         |        c.first_day
         |      FROM
         |        (SELECT
         |            ip,
         |            logintime
         |         FROM tp
         |         GROUP BY 1,2) b
         |    LEFT JOIN
         |      (SELECT
         |          ip,
         |          min(logintime) first_day
         |       FROM
         |           (select
         |                ip,
         |                logintime
         |            FROM
         |                tp
         |            group by 1,2) a
         |       group by 1) c
         |     on b.ip = c.ip
         |     order by 1,2) e
         |  order by 1,2) f
         |group by 1
         |order by 1
        """.stripMargin)
    //saveDay.show()

    // 指定日期1日前留存率
    val oneDB = sQLContext.sql(
      s"""
         |select logintime,count(user_id_d1)/count(ip) retention_d1
         |from(
         |--匹配后续
         |select a.logintime,a.ip,b.ip user_id_d1 from
         |(select logintime,ip from tp a where logintime='${todayDB}')a
         |left join (select * from tp where logintime=date_sub('${todayDB}', 1)) b on a.ip=b.ip)
         |group by logintime
      """.stripMargin)

    // 查询统计登陆次数
    val twoDB = sQLContext.sql(
      """
        |SELECT
        |      ip,
        |      logintime,
        |      first_day,
        |      DATEDIFF(logintime,first_day) as by_day
        |   FROM
        |     (SELECT
        |        b.ip,
        |        b.logintime,
        |        c.first_day
        |      FROM
        |        (SELECT
        |            ip,
        |            logintime
        |         FROM tp
        |         GROUP BY 1,2) b
        |    LEFT JOIN
        |      (SELECT
        |          ip,
        |          min(logintime) first_day
        |       FROM
        |           (select
        |                ip,
        |                logintime
        |            FROM
        |                tp
        |            group by 1,2) a
        |       group by 1) c
        |     on b.ip = c.ip
        |     order by 1,2) e
        |  order by 1,2
      """.stripMargin)
    //twoDB.createOrReplaceTempView("ip_logtime")

    // 3日前 全局活跃率计算
    val joinData = sQLContext.sql(
      """
        |select
        |	a.logintime,
        |	round((saveone/savetwo)*100,2)||'%' thrDB
        |	from
        |		(select
        |			a.logintime logintime,
        |			count(*)+1 saveone
        |	from tp a
        |		join
        |		 tp b
        |		on a.ip=b.ip and a.logintime = date_add(b.logintime,3)
        |		group by a.logintime) a
        |	left join
        |		(select
        |			logintime,
        |			count(*)+1 savetwo
        |		from tp
        |		group by logintime) b
        |	on a.logintime = b.logintime
      """.stripMargin)
    //joinData.show()
    // wau
    val saveSD = sQLContext.sql(
      """
        |select
        |		t.logintime,
        |		t.ip,
        |		(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
        |			when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
        |			when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
        |from (
        |	select
        |		t1.logintime,
        |		t1.ip,
        |		datediff(t1.logintime,t2.logintime) weeks
        |	from
        |		(select *
        |			from tp
        |			where logintime >= to_date('2019-03-01') and logintime < to_date('2019-03-31')) t1
        |	left join
        |	(select  *
        |		from  tp) t2
        |	on
        |	(t1.ip=t2.ip )
        |	where datediff(t1.logintime,t2.logintime)>=0 and datediff(t1.logintime,t2.logintime) <=28
        |	) t
        |	group by t.logintime,t.ip
      """.stripMargin)


    saveSD.show()

    sc.stop()
  }

}


