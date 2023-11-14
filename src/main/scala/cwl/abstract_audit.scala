package cwl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

abstract class abstract_audit {

  val logger: Logger = Logger.getLogger(this.getClass)

  // 关闭日志输出
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  val sparkSession = SparkSession
    .builder()
    .master("yarn")
    .enableHiveSupport()
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer").getOrCreate()

  def queryGp(sql:String):DataFrame = {
    sparkSession
      .read
      .format("jdbc")
      .option("url", "jdbc:postgresql://10.1.60.63:5432/lottery?user=gpadmin&password=!QAZxsw2&useUnicode=true&characterEncoding=utf-8")
      .option("driver", "org.postgresql.Driver")
      .option("query", sql)
      .option("user", "gpadmin")
      .option("password", "gpadmin")
      .load()
  }


  def queryKylin(sql:String): DataFrame ={
    //      val source = new SimpleDataSource("jdbc:kylin://192.168.200.81:7070/cwl_devel_new", "ANALYST", "ANALYST", "org.apache.kylin.jdbc.Driver")
    //      val db = Db.use(source)


    sparkSession.read.format("jdbc")
      .option("url","jdbc:kylin://10.1.60.166:7070/cwldw")
      .option("driver","org.apache.kylin.jdbc.Driver")
      .option("user","ADMIN")
      .option("password","KYLIN")
      .option("query",sql)
      .load()


  }

}
