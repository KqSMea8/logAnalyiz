package testCompute

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import scalaj.http.Http

object uvcompute {

  def main(args: Array[String]): Unit = {

    // 首先判断目录是否为空
    //E:/sessionOutputDemo/1819331/part-00000
    if(args.length != 1){
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
    // lines.cache()
    //进行过滤，要进行特殊处理
    val rowRDD = lines.map(t =>{
      val lines = t.split("\t")
      //println(lines(1)+"_"+lines(4))
      //截取日期
      val day = lines(0).substring(0,10)
      val ip = lines(1)

      day + "\t" + ip + "\t" + lines(2)+ "\t" + lines(3)
    }).filter(t=>t.length != 0).distinct()
    println(rowRDD.count())
    println(rowRDD.distinct().count())
    //存储result文件
    //df.coalesce(1).write.save("E:/sessionOutputDemo/2019")
    rowRDD.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/uvresultip")
    sc.stop()
  }

}
