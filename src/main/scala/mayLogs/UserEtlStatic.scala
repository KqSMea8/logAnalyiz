package mayLogs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Author wenBin
  * Date 2019/6/10 15:35
  * Version 1.0
  * 清洗原始日志
  * 抽取出中心统计记录
  * C:/Users/Administrator/Desktop/用户日志/201905/
  */
object UserEtlStatic {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("目录不正确，退出程序")
      sys.exit()
    }
    //创建一个集合存储输入输出目录
    val Array(inputPath) = args
    val conf = new SparkConf()
      .setAppName(s"${this.getClass.getName}").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sQLContext = new SQLContext(sc)
    //开始读取数据
    val lines = sc.textFile(inputPath)


    val startRdd = lines.filter(rdd => {

      val rdds = rdd.split(" ", 7)


      // 过滤出中心统计数据
      rdds.length >= 5 &&  rdds(5).split("/").length > 3

    }).filter(rdd =>{

      val rdds = rdd.split(" ", 7)

      // 过滤三种统计数据
      val strUrl = rdds(5).split("/")

      "/" + strUrl(1) + "/" + strUrl(2) == "/rest/sjTargetGroup" ||
      "/" + strUrl(1) + "/" + strUrl(2) == "/rest/sjReport" ||
      "/" + strUrl(1) + "/" + strUrl(2) == "/rest/ebdStatisticsResult"
    })

    val resultRdd = startRdd.map(rdd => {

      val rdds = rdd.split(" ", 7)

      // 过滤三种统计数据
      val strUrl = rdds(5).split("/")

      val endUrl = "/" + strUrl(1) + "/" + strUrl(2)

      // date_time ip session url
      rdds(0) +" "+ rdds(1) + "\t" + rdds(2) + "\t" + rdds(3) + "\t" + endUrl
    })

    resultRdd.foreach(println)
    println(resultRdd.count())

    resultRdd.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/201905/统计数据")

    sc.stop()
  }
}
