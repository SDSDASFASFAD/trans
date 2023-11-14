package beijing

import cn.hutool.core.util.StrUtil
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.immutable.HashMap
import scala.collection.mutable
import scala.collection.parallel.immutable

/**
 * 北京省稽核系统站点编码上传规则在21年发生改变：
 *  21年之前站点编码为六位，21年既有六位站点编码又有八位站点编码（在六位站点编码之前加11），21年之后站点编码都是八位
 *
 */

object station_machine_modify {


  def main(args: Array[String]): Unit = {

    val source_table = args(0)
    val target_table = args(1)
    val cwl_id = args(2)
    val dt_start = args(3)
    val dt_end = args(4)

    val ses = SparkSession
      .builder()
      .appName("")
      .master("yarn")
      .enableHiveSupport()
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.exec.dynamic.partition","true")
      .config("hive.exec.dynamic.partition.mode", "nostrick")
      .config("hive.exec.max.dynamic.partitions","10000")
      .config("hive.exec.max.dynamic.partitions.pernode","100000")
      .config("hive.exec.max.created.files","100000")
      .getOrCreate()
    import ses.implicits._

    val frame: DataFrame = ses.sql(
      s"""
         | select * from $source_table where dt >= '$dt_start' and dt < '$dt_end' and cwl_id = '$cwl_id'
         |""".stripMargin).toDF()


    val value: DataFrame = frame.map(row => {
      val id: String = row.getAs[String](0)
      val serial: String = row.getAs[String](1)
      val game_id: Int = row.getAs[Int](2)
      var station_id: String = row.getAs[String](3)
      val sell_issue: String = row.getAs[String](4)
      val valid_issue: String = row.getAs[String](5)
      val station_order: Int = row.getAs[Int](6)
      val order_datetime: String = row.getAs[String](7)
      val order_method: Int = row.getAs[Int](8)
      val order_num: Int = row.getAs[Int](9)
      val total_cost: Int = row.getAs[Int](10)
      var machine_id: String = row.getAs[String](11)
      val status: String = row.getAs[String](12)
      val cancel_time: String = row.getAs[String](13)
      val cancel_flag: String = row.getAs[String](14)
      val print_flag: String = row.getAs[String](15)
      val operator: String = row.getAs[String](16)
      val shop_serial: String = row.getAs[String](17)
      val issue_serial: String = row.getAs[String](18)
      val draw_time: String = row.getAs[String](19)
      val play_type_map: Map[String, String] = row.getAs[Map[String, String]](20)
      val bet_number_map: Map[String, String] = row.getAs[Map[String, String]](21)
      val times_map: Map[String, String] = row.getAs[Map[String, String]](22)
      val total_num_map: Map[String, String] = row.getAs[Map[String, String]](23)
      val total_cost_map: Map[String, String] = row.getAs[Map[String, String]](24)
      val dt: String = row.getAs[String](25)
      val cwl_id: String = row.getAs[String](26)

      if (station_id.length == 6) {
        station_id = "11" + station_id
      }

      if (machine_id.matches("\\d{6}01")) {
        machine_id = "11" + machine_id.substring(0, 6)
      }

      if (machine_id.matches("\\d{8}01")) {
        machine_id = machine_id.substring(0, 8)
      }

      D2_SELL(
        id,
        serial,
        game_id,
        station_id,
        sell_issue,
        valid_issue,
        station_order,
        order_datetime,
        order_method,
        order_num,
        total_cost,
        machine_id,
        status,
        cancel_time,
        cancel_flag,
        print_flag,
        operator,
        shop_serial,
        issue_serial
        , draw_time,
        play_type_map,
        bet_number_map,
        times_map,
        total_num_map,
        total_cost_map,
        dt,
        cwl_id
      )
    }).toDF()



//    value.collect().foreach(println)

    value.createTempView("tmp")


    ses.sql(
      s"""
        |insert overwrite table $target_table partition(dt,cwl_id,issue)
        |select
        |*,
        |valid_issue
        |from tmp
        |""".stripMargin)



  }

  case class D2_SELL(
                      id: String,
                      serial: String,
                      game_id: Int,
                      station_id: String,
                      sell_issue: String,
                      valid_issue: String,
                      station_order: Int,
                      order_datetime: String,
                      order_method: Int,
                      order_num: Long,
                      total_cost: Long,
                      machine_id: String,
                      status: String,
                      cancel_time: String,
                      cancel_flag: String,
                      print_flag: String,
                      operator: String,
                      shop_serial: String,
                      issue_serial: String,
                      draw_time: String,
                      play_type_map:Map[String,String],
                      bet_number_map:Map[String,String],
                      times_map:Map[String,String],
                      total_num_map:Map[String,String],
                      total_cost_map:Map[String,String],
                      dt: String,
                      cwl_id: String
                    )

}
