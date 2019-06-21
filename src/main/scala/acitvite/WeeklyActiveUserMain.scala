//package acitvite
//
//import org.apache.log4j.Logger
//import org.apache.spark.storage.StorageLevel
//
//import scala.collection.mutable.ListBuffer
//
///**
//  * 基于日活跃明细数据,计算周活跃用户
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
//object WeeklyActiveUserMain {
//  val logger: Logger = Logger.getLogger(WeeklyActiveUserMain.getClass)
//  val timeStr = " 00:00:00"
//  val defaultIntervalWeeks = 3
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
//    val from = DateUtils.getFirstDayOfNatureWeek(args(0) + timeStr)
//    val to = DateUtils.addDay(DateUtils.getLastDayOfNatureWeek(args(1) + timeStr), 1)
//    logger.info(s"根据输入的时间范围，得到的周区间边界为{%s-%s}".format(from, to))
//    val sparkSession = SparkSessionUtils.getOrCreate("WeeklyActiveUserMainJob")
//    try {
//      for (appCode <- appCodes) {
//        val beforeFourWeekDay = DateUtils.addWeek(from, -defaultIntervalWeeks)
//        logger.info(s"产品{%s},活跃用户的查询区间为{%s-%s}".format(appCode, beforeFourWeekDay, to))
//        SparkESSimpleActiveDetailsDao.readEsSimpleActiveDetailsData(appCode, beforeFourWeekDay, to, sparkSession, isCache = false)
//        //将活跃用户详情,以一周的第一天为单位，进行用户分组去重
//        val weeklyActiveUserSql =
//          s"""
//             |select date_add(next_day(stat_time, 'Sun'), -7) week_firstday,reg_channel,app_version,os,user_id
//             |from global_temp.simple_user_active_details
//             |group by date_add(next_day(stat_time, 'Sun'), -7),reg_channel,app_version,os,user_id
//          """.stripMargin
//        logger.info("weeklyActiveUserSql：" + weeklyActiveUserSql)
//        val weeklyActiveUserDf = sparkSession.sql(weeklyActiveUserSql).persist(StorageLevel.MEMORY_AND_DISK_SER)
//        weeklyActiveUserDf.createOrReplaceGlobalTempView("weekly_active_user")
//        /**
//          * 第1种 按渠道、版本、平台、用户ID关联（全关联）
//          * 渠道    版本   平台
//          * √      √     √
//          */
//        val weeklyActiveUserSql1 =
//          s"""
//             |select t.week_firstday,t.reg_channel,t.app_version,t.os,t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.reg_channel,t1.app_version,t1.os,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.reg_channel=t2.reg_channel and t1.app_version=t2.app_version and t1.os = t2.os)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.reg_channel,t.app_version,t.os,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user1" + weeklyActiveUserSql1)
//        val weeklyActiveUserDf1 = sparkSession.sql(weeklyActiveUserSql1)
//
//        /**
//          * 第2种 按渠道、版本关联
//          * 渠道    版本   平台
//          * √       √     ×
//          */
//        val weeklyActiveUserSql2 =
//          s"""
//             |select t.week_firstday,t.reg_channel,t.app_version,'${Constants.UNKNOWN}',t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.reg_channel,t1.app_version,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.reg_channel=t2.reg_channel and t1.app_version=t2.app_version)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.reg_channel,t.app_version,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user2" + weeklyActiveUserSql2)
//        val weeklyActiveUserDf2 = sparkSession.sql(weeklyActiveUserSql2)
//
//        /**
//          * 第3种 按渠道、用户ID关联
//          * 渠道    版本   平台
//          * √       ×     ×
//          */
//        val weeklyActiveUserSql3 =
//          s"""
//             |select t.week_firstday,t.reg_channel,'${Constants.UNKNOWN}','${Constants.UNKNOWN}',t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.reg_channel,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.reg_channel=t2.reg_channel)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.reg_channel,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user3" + weeklyActiveUserSql3)
//        val weeklyActiveUserDf3 = sparkSession.sql(weeklyActiveUserSql3)
//
//        /**
//          * 第4种 按渠道、平台、用户ID关联
//          * 渠道    版本   平台
//          * √       ×     √
//          */
//        val weeklyActiveUserSql4 =
//          s"""
//             |select t.week_firstday,t.reg_channel,'${Constants.UNKNOWN}',t.os,t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.reg_channel,t1.os,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.reg_channel=t2.reg_channel and t1.os = t2.os)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.reg_channel,t.os,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user4" + weeklyActiveUserSql4)
//        val weeklyActiveUserDf4 = sparkSession.sql(weeklyActiveUserSql4)
//
//        /**
//          * 第5种 只按用户ID关联
//          * 渠道    版本   平台
//          * ×       ×     ×
//          */
//        val weeklyActiveUserSql5 =
//          s"""
//             |select t.week_firstday,'${Constants.UNKNOWN}','${Constants.UNKNOWN}','${Constants.UNKNOWN}',t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user5" + weeklyActiveUserSql5)
//        val weeklyActiveUserDf5 = sparkSession.sql(weeklyActiveUserSql5)
//
//        /**
//          * 第6种 按平台、用户ID关联
//          * 渠道    版本   平台
//          * ×       ×     √
//          */
//        val weeklyActiveUserSql6 =
//          s"""
//             |select t.week_firstday,'${Constants.UNKNOWN}','${Constants.UNKNOWN}',t.os,t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.os,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.os = t2.os)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.os,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user6" + weeklyActiveUserSql6)
//        val weeklyActiveUserDf6 = sparkSession.sql(weeklyActiveUserSql6)
//        /**
//          * 第7种 按版本、用户ID关联
//          * 渠道    版本   平台
//          * ×       √     ×
//          */
//        val weeklyActiveUserSql7 =
//          s"""
//             |select t.week_firstday,'${Constants.UNKNOWN}',t.app_version,'${Constants.UNKNOWN}',t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.app_version,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.app_version=t2.app_version)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.app_version,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user7" + weeklyActiveUserSql7)
//        val weeklyActiveUserDf7 = sparkSession.sql(weeklyActiveUserSql7)
//        /**
//          * 第8种 按版本、平台、用户ID关联
//          * 渠道    版本   平台
//          * ×       √     √
//          */
//        val weeklyActiveUserSql8 =
//          s"""
//             |select t.week_firstday,'${Constants.UNKNOWN}',t.app_version,t.os,t.user_id,
//             |(case when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,9)='0|7|14|21' then 4
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,6)='0|7|14' then 3
//             | when substr(concat_ws ('|',sort_array(collect_set(distinct weeks))),0,3)='0|7' then 2 else 1 end) as active_weeks
//             |from (
//             |  select t1.week_firstday,t1.app_version,t1.os,t1.user_id,datediff(t1.week_firstday,t2.week_firstday) weeks from
//             |  (select * from global_temp.weekly_active_user where week_firstday >= to_date('$from') and week_firstday < to_date('$to')) t1
//             |  left join  (select  * from  global_temp.weekly_active_user) t2
//             |  on (t1.user_id=t2.user_id and t1.app_version=t2.app_version and t1.os = t2.os)
//             |  where datediff(t1.week_firstday,t2.week_firstday)>=0 and datediff(t1.week_firstday,t2.week_firstday) <=28
//             |) t group by t.week_firstday,t.app_version,t.os,t.user_id
//				""".stripMargin
//        logger.info("weekly_active_user8" + weeklyActiveUserSql8)
//        val weeklyActiveUserDf8 = sparkSession.sql(weeklyActiveUserSql8)
//
//        val unionWeeklyActiveUserRdd = weeklyActiveUserDf1.union(weeklyActiveUserDf2).union(weeklyActiveUserDf3).union(weeklyActiveUserDf4)
//          .union(weeklyActiveUserDf5).union(weeklyActiveUserDf6).union(weeklyActiveUserDf7).union(weeklyActiveUserDf8)
//        val weeklyActiveUserRdd = unionWeeklyActiveUserRdd.rdd.mapPartitions(rdd => {
//          rdd.map(row => {
//            val weekFirstDay = row.get(0).toString + timeStr
//            val channel = row.get(1).toString
//            val appVersion = row.get(2).toString
//            val os = row.get(3).toString
//            val userId = row.get(4).toString
//            val activeWeeks = row.get(5).toString.toInt
//            val id = Utils.hash(appCode + os + channel + appVersion + userId + weekFirstDay)
//            Map("id" -> id, "app_code" -> appCode, "os" -> os, "channel" -> channel, "app_version" -> appVersion, "user_id" -> userId, "stat_time" -> weekFirstDay, "active_weeks" -> activeWeeks)
//          })
//        })
//        val weeklyActiveUserIndex = EdaRouteAdmin.getOrCreateIndex(appCode, Constants.WEEKLY_ACTIVE_USER_DETAILS_TYPE, Constants.WEEKLY_ACTIVE_USER_DETAILS_MAPPING)
//        EsSpark.saveToEs(weeklyActiveUserRdd, weeklyActiveUserIndex.getIndexName + "/" + Constants.WEEKLY_ACTIVE_USER_DETAILS_TYPE, Map("es.mapping.id" -> "id"))
//        EdaRouteAdmin.updateRouteIndex(appCode, Constants.WEEKLY_ACTIVE_USER_DETAILS_TYPE, Constants.WEEKLY_ACTIVE_USER_DETAILS_MAPPING, weeklyActiveUserIndex, "stat_time")
//        weeklyActiveUserDf.unpersist()
//      }
//    } catch {
//      case e: Exception => logger.error("execute error.", e)
//    } finally {
//      sparkSession.stop()
//      EsAdminUtil.closeClient()
//    }
//  }
//}