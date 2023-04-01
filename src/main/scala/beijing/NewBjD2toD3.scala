package beijing

import java.time.LocalDate
import java.util

import com.ilotterytech.ocean.dp.D1D2.d2model
import com.ilotterytech.ocean.dp.D2D3.GameFactory
import com.ilotterytech.ocean.dp.D2D3.d3model.Sell
import com.ilotterytech.ocean.dp.D2D3.server.TransD3Data.sell
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object NewBjD2toD3 {
  def main(args: Array[String]): Unit = {
    val cwl_id=args(0)
    val d2_dt=args(1)
    val start_issue=args(2)
    val end_issue = args(3)
    val d3_dt = args(4)
    val valid_issue = args(5)
    val ses = SparkSession
      .builder()
      .appName("")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()
    import ses.implicits._
    import scala.collection.JavaConverters._
    val rawData: DataFrame = ses.sql(
      s"""
         |SELECT * FROM bjd1d2d3.d2_sell WHERE cwl_id = '$cwl_id' AND dt='$d2_dt' and valid_issue >= '$start_issue' and valid_issue <= '$end_issue' and
         |status = '0'
         |""".stripMargin)

    val value: RDD[Sell] = rawData.rdd.mapPartitions(part => {
      val ga = GameFactory.getInstance().getGame(cwl_id)
      part.flatMap(row => {
        val sellD2: d2model.Sell = hive_d2model(row, cwl_id)
        val sells: util.LinkedList[Sell] = sell(ga, sellD2)
        val scala = sells.asScala
        scala
      })
    })
    val d3: RDD[D3_Sell] = value.map(sell => {
      D3_Sell(
        sell.getCs_index,
        sell.getCs_ticket_code,
        sell.getCs_ticket_mark,
        sell.getCs_ticket_price,
        sell.getCs_price,
        sell.getCs_total_price,
        sell.getCs_frequency,
        sell.getCs_issue_fk,
        sell.getCs_date_fk.toInt,
        sell.getCs_time_fk,
        sell.getCs_cp_fk,
        sell.getCs_s_fk,
        sell.getCs_multiple_fk,
        sell.getCs_pl_fk,
        sell.getCs_ticket_pl_fk,
        sell.getCs_t_offset_fk,
        sell.getCs_w_fk,
        sell.getCs_issue_sell_fk
      )
    })
    val dt = LocalDate.now().toString
    var tmp="tmp"+cwl_id
    d3.toDF().createTempView(tmp)
    ses.sql(
      s"""
         |INSERT OVERWRITE TABLE olap.cs PARTITION(etl_date='$d3_dt',cwlid='$cwl_id',valid_issue='$valid_issue') select * from $tmp
         |""".stripMargin)

  }

  def hive_d2model(row: Row,cwl_id:String):com.ilotterytech.ocean.dp.D1D2.d2model.Sell ={
    val play_type_map: Map[String, String] = row.getAs[Map[String, String]]("play_type_map")
    val bet_number_map: Map[String, String] = row.getAs[Map[String, String]]("bet_number_map")
    val times_map: Map[String,String] = row.getAs[Map[String, String]]("times_map")
    val total_num_map: Map[String,String] = row.getAs[Map[String, String]]("total_num_map")
    val total_cost_map: Map[String, String] = row.getAs[Map[String, String]]("total_cost_map")
    val keySet = play_type_map.keySet
    val infoes: Array[com.ilotterytech.ocean.dp.D1D2.d2model.Sell.BetInfo] = new Array[com.ilotterytech.ocean.dp.D1D2.d2model.Sell.BetInfo](keySet.size)
    for (elem <- keySet) {
      val info: com.ilotterytech.ocean.dp.D1D2.d2model.Sell.BetInfo = com.ilotterytech.ocean.dp.D1D2.d2model.Sell.BetInfo.builder().playType(play_type_map(elem))
        .betNumber(bet_number_map(elem))
        .times(times_map(elem).toInt)
        .totalNum(total_num_map(elem).toInt)
        .totalCost(total_cost_map(elem).toInt).build()
      infoes(elem.toInt)=info
    }
    val sell = com.ilotterytech.ocean.dp.D1D2.d2model.Sell.builder()
      .id("id")
      .serial(row.getAs[String]("serial"))
      .gameId(row.getAs[Integer]("game_id"))
      .stationId(row.getAs[String]("station_id")+"01")
      .sellIssue(row.getAs[String]("sell_issue"))
      .validIssue(row.getAs[String]("valid_issue"))
      .orderDatetime(row.getAs[String]("order_datetime"))
      .orderMethod(row.getAs[Integer]("order_method"))
      .content("")
      .orderNum(row.getAs[Integer]("order_num").toLong)
      .totalCost(row.getAs[Integer]("total_cost").toLong)
//      .cwlCode(cwl_id)
      .eltDate("")
      //销售终端机（投注机）编号，（一般情况下站点只有一台机器，而彩票信息中给的站点编号为 8位数字 ，此种情况时 站点编号+01 = 机器编号）
      .machineId(row.getAs[String]("station_id") + "01")
      .status(row.getAs[String]("status"))
      .cancelTime(row.getAs[String]("cancel_time"))
      .cancelFlag(row.getAs[String]("cancel_flag"))
      .printFlag(row.getAs[String]("print_flag"))
      .operator(row.getAs[String]("operator"))
      .shop_serial("")
      .issue_serial(row.getAs[String]("issue_serial"))
      .drawTime("")
      .betInfos(infoes).build()
    sell
  }

  case class D3_Sell(
                      cs_index:Int,
                      cs_ticket_code:String,
                      cs_ticket_mark:Int,
                      cs_ticket_price:Long,
                      cs_price:Int,
                      cs_total_price:String,
                      cs_frequency:Int,
                      cs_issue_fk:Long,
                      cs_date_fk:Int,
                      cs_time_fk:Int,
                      cs_cp_fk:Int,
                      cs_s_fk:Long,
                      cs_multiple_fk:Int,
                      cs_pl_fk:Int,
                      cs_ticket_pl_fk:String,
                      cs_t_offset_fk:Int,
                      cs_w_fk:Long,
                      cs_issue_sell_fk:Long
                    )
}
