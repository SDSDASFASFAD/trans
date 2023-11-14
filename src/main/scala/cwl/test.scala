package cwl

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.PropertyConfigurator

import java.io.InputStream
import java.util.Properties

object test {

//  val logger: Logger = Logger.getLogger(this.getClass)



  def main(args: Array[String]): Unit = {
//        org.apache.log4j.PropertyConfigurator
//          .configure("resources/log4j.properties")

//    val props = new Properties()
//    val inputStream: InputStream = getClass.getClassLoader.getResourceAsStream("log4j.properties")
//    props.load(inputStream)
//    PropertyConfigurator.configure(props)

    // 关闭日志输出
//    Logger.getLogger("org").setLevel(Level.OFF)
//    Logger.getLogger("akka").setLevel(Level.OFF)

//    System.setProperty("hadoop.home.dir","F:\\Hadoop\\hadoop-2.7.1")

    val session = SparkSession.builder()
      .master("local")
      .getOrCreate()


    session.sparkContext.setLogLevel("ERROR")

    val value = session
      .sparkContext.makeRDD(List("双色球第2023033期"))

    value.map(line => {

      val strings = line.replace("第", "\t第").split("\t")
      strings(0)

    }).foreach(println)
  }

}
