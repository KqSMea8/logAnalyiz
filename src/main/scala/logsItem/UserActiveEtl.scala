package logsItem

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import utils.SchemaUtils

object UserActiveEtl {

  def main(args: Array[String]): Unit = {

    // 首先判断目录是否为空
    //C:/sessionDemo/*
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
    //进行过滤，要进行特殊处理
    val rowRDD = lines.map(rdd =>{
      val t = rdd.split("\"")
      val de_te_ip_session = t(0).split(" ")
      var login = ""
      var shebei = ""
      if (1 < t(1).split(" ").length){
        if(3 < t(1).split(" ")(1).split("/").length){
          if (5 <= t(1).split(" ")(1).split("/")(3).length){
            login = t(1).split(" ")(1).split("/")(3).substring(0,5)
          }
          if(4 < t(2).split(" ").length){
            shebei = t(2).split(" ")(4).replace("(","").replace(";","")
          }
          de_te_ip_session(0)+" "+de_te_ip_session(1)+"\t"+de_te_ip_session(2)+"\t"+de_te_ip_session(3).replace("-","null")+"\t"+login + "\t" +shebei
        }else {
          ""
        }
      }else{
        ""
      }
    }).filter(t=>t.length != 0)
      .filter(t=> t.split("\t").length > 3 && t.split("\t")(3).equals("login") && !t.split("\t")(1).equals("-"))
      .distinct()
    val scheml: RDD[Row] = rowRDD.map(t => {
      val lines = t.split("\t")
      Row(
        lines(0),
        lines(1),
        lines(2),
        lines(3),
        lines(4)
      )
    })
    val df = sQLContext.createDataFrame(scheml,SchemaUtils.schema)
    //存储result文件
    rowRDD.coalesce(1).saveAsTextFile("E:/sessionOutputDemo/1819431")
    sc.stop()
  }
}
