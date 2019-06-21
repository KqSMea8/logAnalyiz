package mayLogs

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

/**
  * Author wenBin
  * Date 2019/6/10 15:35
  * Version 1.0
  * 清洗原始日志
  * 抽取出上报数据   /rest/sjExperimentResults/projectList
  * C:/Users/Administrator/Desktop/用户日志/201905/
  */
object UserEtlReport {


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

      // 过滤上报实验室上报数据
      rdds.length >= 5 && rdds(5).split("\\?", 2)(0) == "/rest/sjExperimentResults/projectList"
    })

    val resultRdd = startRdd.map(rdd => {
      val rdds = rdd.split(" ", 7)

      val endUrl = rdds(5).split("\\?", 2)(0)

      // date_time ip session url
      rdds(0) +" "+ rdds(1) + "\t" + rdds(2) + "\t" + rdds(3) + "\t" + endUrl
    })

    resultRdd.foreach(println)
    println(resultRdd.count())


    resultRdd.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/201905/上报数据")

    sc.stop()
  }

}
