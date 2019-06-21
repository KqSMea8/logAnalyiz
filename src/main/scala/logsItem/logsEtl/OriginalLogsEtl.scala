package logsItem.logsEtl

import eu.bitwalker.useragentutils.UserAgent
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author wenBin
  * Date 2019/6/18 9:39
  * Version 1.0
  * from tomcat gain logs
  * starting first step deal with
  * E:/用户日志/tomcat02版/ E:/sessionOutputDemo/originallogs
  *
  */
object OriginalLogsEtl {

  def main(args: Array[String]): Unit = {

    // judge input output path is not null
    if (args.length != 2) {
      println("Directory is Error!")
      sys.exit()
    }

    // create arr save input、output path
    var Array(inputPath, outputPath) = args
    // create application starting
    val conf = new SparkConf().setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // file input
    val fileRdd = sc.textFile(inputPath)

    // rddlines deal with
    val firstRdd = fileRdd.map(rdd => {

      val lines = rdd.split(" ", 11)
      if (lines.length == 11) {
        // date ip url windows
        if (!lines(5).equals("/")) {

          lines(0) + " " + lines(1) + "\t" + lines(2) + "\t" +
            lines(5).replaceAll("//","/") + "\t" + lines(10)
        } else {
          ""
        }
      } else {
        ""
      }
    }).filter(rdd => !rdd.isEmpty)

    //deal with date and ip
    val dateipRdd = firstRdd.map(rdd => {
      val lines = rdd.split("\t")

      if (19 == lines(0).length) {
        if (9 <= lines(1).length) {

          rdd
        } else {
          ""
        }
      } else {
        ""
      }
    }).filter(rdd => !rdd.isEmpty)


    // deal with url
    val secRDD = dateipRdd.map(rdd => {

      val lines = rdd.split("\t")

      val urletl = lines(2)

      if (urletl.length >= 10) {
        if (urletl.substring(0, 10).equals("/resources") ||
          urletl.substring(urletl.length - 4, urletl.length).equals(".ico") ||
          urletl.substring(urletl.length - 4, urletl.length).equals(".jpg") ||
          urletl.substring(urletl.length - 4, urletl.length).equals(".txt") ||
          urletl.substring(urletl.length - 4, urletl.length).equals(".png") ||
          !urletl.contains("/rest")) {
          ""
        } else {
          rdd
        }
      } else {
        ""
      }

    }).filter(rdd => !rdd.isEmpty)

    // deal with user-agent
    val resultRdd = secRDD.map(rdd => {

      val lines = rdd.split("\t")
      if (lines.length == 4) {

        val user_agent = lines(3)
        val agent = UserAgent.parseUserAgentString(user_agent)

        //
        lines(0) + "\t" + lines(1) + "\t" + lines(2) + "\t" +
          agent.getOperatingSystem + "\t" + agent.getBrowser
      }else{

        lines(0) + "\t" + lines(1) + "\t" + lines(2) + "\t" +
          "UNKNOWN" + "\t" + "UNKNOWN"
      }
    }).filter(rdd => !rdd.isEmpty)


    // save result data to filepath
    resultRdd.coalesce(10).saveAsTextFile(outputPath)

    // stop application
    sc.stop()
  }

}
