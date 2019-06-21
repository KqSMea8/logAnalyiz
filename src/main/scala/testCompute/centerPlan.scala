package testCompute

import java.text.SimpleDateFormat
import java.util.Locale

import scala.util.Random

object centerPlan {

  def main(args: Array[String]): Unit = {

    // 原始试剂
    val nums = List("a", "b", "c", "d", "e", "f", "j")
    // 对照表
    var duimap: Map[String, Int] = Map()
    nums.foreach(t => {
      val a = Random.nextInt(7)
      duimap += ( t -> a )
    })
    //duimap.foreach(println)

  // 下发实验室
  // 100家实验室
  var map: Map[Int, Int] = Map()
  for (i <- 1 to 100) {
    var a = Random.nextInt(7)
    map += (i -> a)
    //println(i)
  }
  //map.foreach(println)

    val loc = new Locale("what")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", loc)
    val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val format1 = new SimpleDateFormat("yyyy-MM-dd")

    val timeaa: Long = 1557702024
    val newTime = format.format(timeaa*1000)
    //println(newTime)

    var strings = "2222小时前"
    println(strings.substring(0,strings.length-3))


    var stringss = "2019-06-04"
//    println(stringss.replace("年", "-")
//      .replace("月", "-")
//      .replace("日", ""))

    println(format1.parse(stringss))


}

}
