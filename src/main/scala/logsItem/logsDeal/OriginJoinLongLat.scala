package logsItem.logsDeal

import dao.IPSeeker
import org.apache.spark.sql._
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/6/18 16:58
  * Version 1.0
  *
  * originalData Join Longitude and Latitude
  */
object OriginJoinLongLat {

  def main(args: Array[String]): Unit = {

    // judge input output is two
    if (args.length != 2) {

      println("Directory is ERROR!")
      sys.exit()
    }

    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)


    val Array(inputPath, outputPath) = args
    val originalRdd = sc.textFile(inputPath)
    // read ip->city information
    val iplocalRdd = sc.textFile("E:\\sessionOutputDemo\\gainiplonglatitude\\part-00000")

    // Use the sample to instantiate the IP data
    val iplocal = iplocalRdd.map(t => {
      val lines = t.split("\t")
      iptocity(lines(0), lines(1), lines(2), lines(3), lines(4))
    })
    //attention: need to import an implicit transformation
    import sQLContext.implicits._
    val iptable: DataFrame = iplocal.toDF()

    // register temp table
    iptable.registerTempTable("localTable")

    // deal with orginalData
    val original = originalRdd.map(rdd => {
      val lines = rdd.split("\t")
      originaldata(lines(0), lines(1), lines(2), lines(3), lines(4))
    })
    val originalTable = original.toDF()

    // register temp table
    originalTable.registerTempTable("originalTable")

    // localTable join originalTable for ip
    val resultTable = sQLContext.sql(
      """
        |select
        |     substr(datetime,0,10) as datetimes,originalTable.ip,url,equipment,browsers,city,region,latitude,longitude
        |   from originalTable  left join localTable  on originalTable.ip = localTable.ip
      """.stripMargin)

    resultTable.registerTempTable("lastTable")

    /**
      * 过滤出login登录
      */
    val loginstatus = sQLContext.sql(
      """
        |select * from
        |       (select datetimes,ip,max(city) as city,count(*) as logincount from
        |         lastTable where url like '%login%' group by  datetimes,ip) a
        | where a.city = 'null' sort by logincount desc
      """.stripMargin)
    //loginstatus.show()
    //println(loginstatus.count())

    /**
      * 过滤出空ip
      */
    val cityNull = sQLContext.sql(
      """
        |select ip,city,latitude,longitude from lastTable where city = 'null' group by ip,city,latitude,longitude
      """.stripMargin)



    val dealNullCity = cityNull.rdd.map(rdd => {
      val ip = rdd.get(0)

      val pSeeker = new IPSeeker
      val ipinformation = pSeeker.getAddress(ip.toString)

       ip +"\t"+ ipinformation + "\t" + rdd.get(3)+ "," + rdd.get(2)
    })
    dealNullCity.foreach(println)
    println(dealNullCity.count())
    /**
      * 过滤出室内操作
      */
    val snStatic = sQLContext.sql(
      """
        |select * from lastTable where url like '%sn%'
      """.stripMargin)
    //    snStatic.show()
    //    println(snStatic.count())

    /**
      * 过滤出室间操作
      */
    val sjStatic = sQLContext.sql(
      """
        |select * from lastTable where url like '%sj%'
      """.stripMargin)
    //sjStatic.show()
    //println(sjStatic.count())


    val transforRdd = resultTable.map(rdd => {
      rdd.get(0) + "\t" + rdd.get(1) + "\t" + rdd.get(2) + "\t" + rdd.get(3) + "\t" + rdd.get(4) + "\t" +
        rdd.get(5) + "\t" + rdd.get(6) + "\t" + rdd.get(7) + "\t" + rdd.get(8)
    })

    // save result data to file
    //transforRdd.coalesce(10).write.mode(SaveMode.Overwrite).text(outputPath)

    // stop application
    sc.stop()
  }

  case class iptocity(ip: String, city: String, region: String, latitude: String, longitude: String)

  case class originaldata(datetime: String, ip: String, url: String, equipment: String, browsers: String)

}

