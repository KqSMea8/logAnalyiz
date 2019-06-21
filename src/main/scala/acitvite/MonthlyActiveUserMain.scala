//package acitvite
//
//import org.apache.log4j.Logger
//import org.apache.spark.storage.StorageLevel
//
//import scala.collection.mutable.ListBuffer
//
///**
//  * 月活跃不能基于周活跃用户数据进行计算，还是要基于日活跃数据进行计算。
//  * 是因为，自然周存在跨月的情况。例如：2018-11-01日的活跃用户，将存入在周日期为2018-10-28日中；
//  * 这样，当根据周活跃数据计算月活跃的时候，就会把11月的活跃用户算到10月分区。
//  *
//  * 计算用户8种维度的关联情况
//  * 渠道    版本   平台
//  * √       √     √
//  * √       √     ×
//  * √       ×     ×
//  * √       ×     √
//  * ×       ×     ×
//  * ×       ×     √
//  * ×       √     ×
//  * ×       √     √
//  */
//object MonthlyActiveUserMain {
//  val logger: Logger = Logger.getLogger(MonthlyActiveUserMain.getClass)
//  val timeStr = " 00:00:00"
//  val defaultIntervalMonths = 3
//
//  def main(args: Array[String]): Unit = {
//    if (args.length < 2) {
//      logger.error(s"输入的参数不正确，from：开始时间，to：结束时间 ")
//      return
//    }
//    val appCodes = ListBuffer[String]()
//    if (args.length > 2) {
//      args(2).split(",").foreach(appCodes.append(_))
//    } else {
//      DataCacheManager.getInstance().getAppkeyAndAppCodeCache.values().foreach(appCodes.append(_))
//    }
//    logger.info(s"输入的时间范围为{%s-%s}，执行的产品为{%s}".format(args(0), args(1), appCodes.mkString(",")))
//    val from = DateUtils.getFirstDayOfMonth(args(0) + timeStr)
//    val to = DateUtils.addDay(DateUtils.getLastDayOfMonth(args(1) + timeStr), 1)
//    logger.info(s"根据输入的时间范围，得到的月区间边界为{%s-%s}".format(from, to))
//    val sparkSession = SparkSessionUtils.getOrCreate("MonthlyActiveUserMainJob")
//    try {
//      for (appCode <- appCodes) {
//        val beforeFourMonthDay = DateUtils.addMonth(from, -defaultIntervalMonths)
//        logger.info(s"当前appCode为{%s},活跃用户的查询区间为{%s-%s}".format(appCode, beforeFourMonthDay, to))
//        SparkESSimpleActiveDetailsDao.readEsSimpleActiveDetailsData(appCode, beforeFourMonthDay, to, sparkSession, isCache = false)
//        //将周活跃用户,以月为单位，进行用户分组去重
//        val monthlyActiveUserSql =
//          s"""
//             |select trunc(stat_time, 'MM') month_firstday,reg_channel channel,app_version,os,user_id
//             |from global_temp.simple_user_active_details
//             |group by trunc(stat_time, 'MM'),reg_channel,app_version,os,user_id
//          """.stripMargin
//        logger.info("monthlyActiveUserSql：" + monthlyActiveUserSql)
//        val monthlyActiveUserDf = sparkSession.sql(monthlyActiveUserSql).persist(StorageLevel.MEMORY_AND_DISK_SER)
////        monthlyActiveUserDf.show()
//        monthlyActiveUserDf.createOrReplaceGlobalTempView("monthly_active_user")
//        /**
//          * 第1种 按渠道、版本、平台、用户ID关联（全关联）
//          * 渠道    版本   平台
//          * √      √     √
//          */
//        val monthlyActiveUserSql1 =
//          s"""
//             |select t.month_firstday,t.channel,t.app_version,t.os,t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.channel,t1.app_version,t1.os,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.channel=t2.channel and t1.app_version=t2.app_version and t1.os = t2.os)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.channel,t.app_version,t.os,t.user_id
//				""".stripMargin
//        logger.info("monthlyActiveUserSql1" + monthlyActiveUserSql1)
//        val monthlyActiveUserDf1 = sparkSession.sql(monthlyActiveUserSql1)
////        monthlyActiveUserDf1.show()
//        /**
//          * 第2种 按渠道、版本关联
//          * 渠道    版本   平台
//          * √       √     ×
//          */
//        val monthlyActiveUserSql2 =
//          s"""
//             |select t.month_firstday,t.channel,t.app_version,'${Constants.UNKNOWN}',t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.channel,t1.app_version,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.channel=t2.channel and t1.app_version=t2.app_version)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.channel,t.app_version,t.user_id
//				""".stripMargin
//        logger.info("monthly_active_user2" + monthlyActiveUserSql2)
//        val monthlyActiveUserDf2 = sparkSession.sql(monthlyActiveUserSql2)
////        monthlyActiveUserDf2.show()
//
//        /**
//          * 第3种 按渠道、用户ID关联
//          * 渠道    版本   平台
//          * √       ×     ×
//          */
//        val monthlyActiveUserSql3 =
//          s"""
//             |select t.month_firstday,t.channel,'${Constants.UNKNOWN}','${Constants.UNKNOWN}',t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.channel,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.channel=t2.channel)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.channel,t.user_id
//				""".stripMargin
//        logger.info("monthly_active_user3" + monthlyActiveUserSql3)
//        val monthlyActiveUserDf3 = sparkSession.sql(monthlyActiveUserSql3)
////        monthlyActiveUserDf3.show()
//
//        /**
//          * 第4种 按渠道、平台、用户ID关联
//          * 渠道    版本   平台
//          * √       ×     √
//          */
//        val monthlyActiveUserSql4 =
//          s"""
//             |select t.month_firstday,t.channel,'${Constants.UNKNOWN}',t.os,t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.channel,t1.os,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.channel=t2.channel and t1.os = t2.os)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.channel,t.os,t.user_id
//				""".stripMargin
//        logger.info("monthly_active_user4" + monthlyActiveUserSql4)
//        val monthlyActiveUserDf4 = sparkSession.sql(monthlyActiveUserSql4)
////        monthlyActiveUserDf4.show()
//
//        /**
//          * 第5种 只按用户ID关联
//          * 渠道    版本   平台
//          * ×       ×     ×
//          */
//        val monthlyActiveUserSql5 =
//          s"""
//             |select t.month_firstday,'${Constants.UNKNOWN}','${Constants.UNKNOWN}','${Constants.UNKNOWN}',t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.user_id
//				""".stripMargin
//        logger.info("monthly_active_user5" + monthlyActiveUserSql5)
//        val monthlyActiveUserDf5 = sparkSession.sql(monthlyActiveUserSql5)
////        monthlyActiveUserDf5.show()
//
//        /**
//          * 第6种 按平台、用户ID关联
//          * 渠道    版本   平台
//          * ×       ×     √
//          */
//        val monthlyActiveUserSql6 =
//          s"""
//             |select t.month_firstday,'${Constants.UNKNOWN}','${Constants.UNKNOWN}',t.os,t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.os,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.os = t2.os)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.os,t.user_id
//				""".stripMargin
//        logger.info("monthly_active_user6" + monthlyActiveUserSql6)
//        val monthlyActiveUserDf6 = sparkSession.sql(monthlyActiveUserSql6)
////        monthlyActiveUserDf6.show()
//        /**
//          * 第7种 按版本、用户ID关联
//          * 渠道    版本   平台
//          * ×       √     ×
//          */
//        val monthlyActiveUserSql7 =
//          s"""
//             |select t.month_firstday,'${Constants.UNKNOWN}',t.app_version,'${Constants.UNKNOWN}',t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.app_version,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.app_version=t2.app_version)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.app_version,t.user_id
//				""".stripMargin
//        logger.info("monthly_active_user7" + monthlyActiveUserSql7)
//        val monthlyActiveUserDf7 = sparkSession.sql(monthlyActiveUserSql7)
////        monthlyActiveUserDf7.show()
//        /**
//          * 第8种 按版本、平台、用户ID关联
//          * 渠道    版本   平台
//          * ×       √     √
//          */
//        val monthlyActiveUserSql8 =
//          s"""
//             |select t.month_firstday,'${Constants.UNKNOWN}',t.app_version,t.os,t.user_id,
//             |(case when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2|3')=1 then 4
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1|2')=1 then 3
//             | when instr(concat_ws ('|',sort_array(collect_set(distinct months))),'0|1')=1 then 2 else 1 end) as active_months
//             |from (
//             |  select t1.month_firstday,t1.app_version,t1.os,t1.user_id,ceil(months_between(t1.month_firstday,t2.month_firstday)) months from
//             |  (select * from global_temp.monthly_active_user where month_firstday >= to_date('$from') and month_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.monthly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.app_version=t2.app_version and t1.os = t2.os)
//             |  where datediff(t1.month_firstday,t2.month_firstday)>=0
//             |) t group by t.month_firstday,t.app_version,t.os,t.user_id
//				""".stripMargin
//        logger.info("monthly_active_user8" + monthlyActiveUserSql8)
//        val monthlyActiveUserDf8 = sparkSession.sql(monthlyActiveUserSql8)
////        monthlyActiveUserDf8.show()
//
//        val unionMonthlyActiveUserRdd = monthlyActiveUserDf1.union(monthlyActiveUserDf2).union(monthlyActiveUserDf3).union(monthlyActiveUserDf4)
//          .union(monthlyActiveUserDf5).union(monthlyActiveUserDf6).union(monthlyActiveUserDf7).union(monthlyActiveUserDf8)
//        val monthlyActiveUserRdd = unionMonthlyActiveUserRdd.rdd.mapPartitions(rdd => {
//          rdd.map(row => {
//            val monthFirstDay = row.get(0).toString + timeStr
//            val channel = row.get(1).toString
//            val appVersion = row.get(2).toString
//            val os = row.get(3).toString
//            val userId = row.get(4).toString
//            val activeMonths = row.get(5).toString.toInt
//            val id = Utils.hash(appCode + os + channel + appVersion + userId + monthFirstDay)
//            Map("id" -> id, "app_code" -> appCode, "os" -> os, "channel" -> channel, "app_version" -> appVersion,
//              "user_id" -> userId, "stat_time" -> monthFirstDay, "active_months" -> activeMonths)
//          })
//        })
//        logger.info("count：" + monthlyActiveUserRdd.count())
//        val monthlyActiveUserIndex = EdaRouteAdmin.getOrCreateIndex(appCode, Constants.MONTHLY_ACTIVE_USER_DETAILS_TYPE, Constants.MONTHLY_ACTIVE_USER_DETAILS_MAPPING)
//        EsSpark.saveToEs(monthlyActiveUserRdd, monthlyActiveUserIndex.getIndexName + "/" + Constants.MONTHLY_ACTIVE_USER_DETAILS_TYPE, Map("es.mapping.id" -> "id"))
//        EdaRouteAdmin.updateRouteIndex(appCode, Constants.MONTHLY_ACTIVE_USER_DETAILS_TYPE, Constants.MONTHLY_ACTIVE_USER_DETAILS_MAPPING, monthlyActiveUserIndex, "stat_time")
//        monthlyActiveUserDf.unpersist()
//      }
//    } catch {
//      case e: Exception => logger.error("execute error.", e)
//    } finally {
//      sparkSession.stop()
//      EsAdminUtil.closeClient()
//    }
//  }
//}
