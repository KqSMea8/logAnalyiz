//package acitvite
//
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.SparkSession
//import org.slf4j.LoggerFactory
//
//import scala.collection.mutable.ListBuffer
//
///**
//  * Created by kequan on 10/11/18.
//  *
//  * 活跃用户: 活跃和注册join 使用频繁，而且消耗资源，做成中间表，给后续的任务使用
//  *
//  * 参数说明：
//  * args(0) ：  格式： 2018-01-01  开始时间
//  * args(1) ：  格式： 2018-02-01  结束时间
//  *
//  * 包含任务和注意细节：
//  * 活跃用户，用户新鲜度(days>=31 的时候 days=31)，留存明细(days)，
//  * 付费习惯（is_pay,amount: arppu=pay_amount(支付金额)/active_count（活跃人数）,arpu=pay_amount(支付金额)/pay_count（支付人数） ）,
//  * 终端分析，地域分析
//  *
//  */
//object ActiveDetailsMain {
//  val timeStr = " 00:00:00"
//  val logger = LoggerFactory.getLogger(ActiveDetailsMain.getClass)
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
//    val from = args(0) + timeStr
//    val to = args(1) + timeStr
//    logger.info(s"执行的产品为{%s}".format(appCodes.mkString(",")))
//    val sparkSession: SparkSession = SparkSessionUtils.getOrCreate("ActiveDetailsMain")
//    try {
//      for (appCode <- appCodes) {
//        logger.info(s"当前计算appCode为{%s}，输入的时间范围为{%s ~ %s}".format(appCode, args(0), args(1)))
//        // 1. 读取数据
//        // 注册数据
//        val registerData = SparkEsUserRegisterDao.readEsUserRegisterData(appCode, DateUtils.addDay(from, -365 * 100), to, sparkSession, isCache = true)
//        // 登录数据
//        SparkESCommonDao.readAppEsData(appCode, PresetEventCode.LOGIN.getValue, from, to, sparkSession, false)
//        // 会话数据
//        SparkESSessionDao.readEsSessionData(appCode, from, to, sparkSession, false)
//        // 支付数据
//        SparkESPaymentDao.readEsSuccessPaymentData(appCode, from, to, sparkSession, false)
//
//        // 2. 明细数据
//        val activeDetailsSql =
//          s"""
//             |select
//             |ucl.app_code,ucl.stat_time stat_time,ucl.channel,ucl.app_version,ucl.os,ucl.user_id,
//             |IFNULL(ur.channel,'') reg_channel,IFNULL(ur.app_version,'') reg_app_version,
//             |IFNULL(ur.os,'') reg_os, IFNULL(ur.stat_time,'${Constants.DEFAULT_DATETIME}')  reg_stat_time,
//             |datediff(to_date(ucl.stat_time), IFNULL(to_date(ur.stat_time),'${Constants.DEFAULT_DATE}')) days,
//             |ucl.is_pay,ucl.amount,ucl.country,ucl.province,ucl.city,ucl.carrier, ucl.device_model
//             |from
//             |(select app_code,user_id,channel,app_version,os,stat_time,max(is_pay) is_pay,round(sum(amount), 2) amount,country,province,city,carrier,device_model from
//             |    (
//             |      select app_code,user_id,channel,app_version,os,concat(substring(stat_time,0,13),':00:00') stat_time,0 is_pay,0 amount,country,province,city,carrier,device_model from global_temp.login where user_id <> '' and user_id is not null
//             |      union
//             |      select app_code,user_id,channel,app_version,os,concat(substring(stat_time,0,13),':00:00') stat_time,0 is_pay,0 amount,country,province,city,carrier,device_model from global_temp.session where user_id <> '' and user_id is not null
//             |      union
//             |      select app_code,user_id,channel,app_version,os,concat(substring(stat_time,0,13),':00:00') stat_time,0 is_pay,0 amount,country,province,city,carrier,device_model from global_temp.user_register where stat_time>='$from' and stat_time<'$to' and user_id <> '' and user_id is not null
//             |      union
//             |      select app_code,user_id,channel,app_version,os,concat(substring(stat_time,0,13),':00:00') stat_time,1 is_pay,amount,country,province,city,carrier,device_model from global_temp.payment where user_id <> '' and user_id is not null
//             |    ) ucl_pre where  user_id  <> '' and user_id is not null
//             |    group by app_code,user_id,channel,app_version,os,stat_time,country,province,city,carrier,device_model
//             |) ucl
//             |left join (
//             |  select app_code,channel,app_version,os,stat_time,user_id from global_temp.user_register where user_id <> '' and user_id is not null
//             |) ur on ur.app_code=ucl.app_code and ur.user_id=ucl.user_id
//             |where datediff(ucl.stat_time,ur.stat_time) >=0 or ur.stat_time is null
//				""".stripMargin
//        logger.info("sql_active_details" + activeDetailsSql)
//
//        // 3. 存入  es
//        val rdd_user_activity: RDD[Map[String, Any]] =  sparkSession.sql(activeDetailsSql).rdd.map(row => {
//          val appCode = row.get(0).toString
//          val statTime = row.get(1).toString
//          val channel = row.get(2).toString
//          val appVersion = row.get(3).toString
//          val os = row.get(4).toString
//          val userId = row.get(5).toString
//          val regChannel = row.get(6).toString
//          val regAppVersion = row.get(7).toString
//          val regOs = row.get(8).toString
//          val regStatTime = row.get(9).toString
//          var days = row.get(10).toString.toInt
//          if (days >= 31) {
//            days = 31
//          }
//          val isPay = row.get(11).toString.toInt
//          val amount = row.get(12).toString.toFloat
//          val country = row.get(13).toString
//          val province = row.get(14).toString
//          val city = row.get(15).toString
//          val carrier = row.get(16).toString
//          val deviceModel = row.get(17).toString
//          val id = Utils.hash(appCode + statTime + channel + appVersion + os + country + province + city + userId + carrier + deviceModel)
//          Map("app_code" -> appCode, "stat_time" -> statTime, "channel" -> channel, "app_version" -> appVersion, "os" -> os,
//            "user_id" -> userId, "reg_channel" -> regChannel, "reg_app_version" -> regAppVersion, "reg_os" -> regOs,
//            "reg_stat_time" -> regStatTime, "days" -> days, "is_pay" -> isPay, "amount" -> amount, "country" -> country,
//            "province" -> province, "city" -> city, "carrier" -> carrier, "device_model" -> deviceModel, "id" -> id)
//        })
//        val userActivityIndex = EdaRouteAdmin.getOrCreateIndex(appCode, Constants.USER_ACTIVE_DETAILS_TYPE, Constants.USER_ACTIVE_DETAILS_MAPPING)
//        EsSpark.saveToEs(rdd_user_activity, userActivityIndex.getIndexName + "/" + Constants.USER_ACTIVE_DETAILS_TYPE, Map("es.mapping.id" -> "id"))
//        EdaRouteAdmin.updateRouteIndex(appCode, Constants.USER_ACTIVE_DETAILS_TYPE, Constants.USER_ACTIVE_DETAILS_MAPPING, userActivityIndex, "stat_time")
//        registerData.unpersist()
//      }
//    } catch {
//      case e: Exception => logger.error("execute error.", e)
//    } finally {
//      sparkSession.stop()
//      EsAdminUtil.closeClient()
//    }
//  }
//}