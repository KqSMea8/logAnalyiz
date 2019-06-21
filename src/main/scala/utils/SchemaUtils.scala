package utils

import org.apache.spark.sql.types._

object SchemaUtils {
  //定义日志的元数据信息
  val schema = StructType(
    Array(
      StructField("date",StringType,true),
      StructField("ip",StringType,true),
      StructField("sessionId",StringType,true),
      StructField("operator",StringType,true),
      StructField("shebei",StringType,true)
    )
  )
}
