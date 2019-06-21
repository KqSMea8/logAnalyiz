package testCompute

import java.text.SimpleDateFormat
import java.util.Locale

object stringDemo {
  def main(args: Array[String]): Unit = {

    val arr = "/"
    val strings = arr.split("/")
    //println(strings.length)
    // strings.foreach(println)

    val loc = new Locale("what")
    val fm = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss",loc)
    val arr1 = "31/Jul/2018:19:01:51"
    val dt2 = fm.parse(arr1).getTime();
    val newtime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(dt2)
    println(newtime)


    val array = "[29/Jul/2016:19:01:51 +0800]".substring(1,21)
    //println(array)

    val arras = "[29/Jul/2016:19:01:51 +0800]"
    //println(arras.length)

    val asdfa = "111.202.75.178 - - [29/Jul/2016:23:23:30 +0800]".split(" ",4)
    //asdfa.foreach(println)

    val line = "101.199.108.59 - - [29/Jul/2016:19:01:51 +0800] \"GET /zhiping/rest/page/login;JSESSIONID=63e3da32-1cff-4671-9604-50cf29993853 HTTP/1.1\" 200 3945"
    val str = line.split("\"")(1).split(" ")(1).split("/")(4).substring(0,5)
    //println(str)

    //println("1532970888000".length)
  }

}
