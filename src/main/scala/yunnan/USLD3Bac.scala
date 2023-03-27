package yunnan

import com.ilotterytech.ocean.dp.D1D2.d2model.Sell
import com.ilotterytech.ocean.dp.D2D3.server.TransD3Data
import com.ilotterytech.ocean.dp.D2D3.{Game, d3model}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.joda.time.LocalDate

import java.util
import java.util.UUID
import scala.collection.JavaConverters.asScalaBufferConverter

object USLD3Bac {
  def main(args: Array[String]): Unit = {
    val cwl_id = args(0)
//    val start_issue = args(1)
//    val end_issue=args(2)
    val d2_table=args(1)
    val d3_table= args(2)
    val etl_date=args(3)
//    val cwl_id = "10003"
//    val start_issue = "2022001"
//    val end_issue="2022001"
//    val d2_table="test.yn_d2_sales"
//    val d3_table= "test.yn_cs"
//    val etl_date= "2023-02-23"
    val ses = SparkSession.builder()
      .master("yarn")
      .enableHiveSupport()
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nostrick")
      .getOrCreate()
    import ses.implicits._
//      val rawData = ses.sql(
//        s"""
//           |SELECT * FROM  d2.cwl_d2_usl_sales  WHERE cwl_id=$cwl_id   AND issue=$issue AND valid_issue=$issue AND
//           |status = '0' or status='2'
//           |""".stripMargin)
//    val rawData = ses.sql(
//      s"""
//         |SELECT * FROM  d2.cwl_d2_usl_sales  WHERE cwl_id=$cwl_id   AND issue>=$issue and issue<=$valid_issue1 AND valid_issue>=$issue and valid_issue<=$valid_issue1 AND
//         |(status = '0' or status='2')
//         |""".stripMargin)
    val rawData = ses.sql(
      s"""
         |SELECT * FROM  $d2_table  WHERE etl_date='$etl_date' AND (status = '0' or status='2')
         |""".stripMargin)
      val value: RDD[d3model.Sell] = rawData.rdd.mapPartitions(part => {
        val ga: Game = com.ilotterytech.ocean.dp.D2D3.GameFactory.getInstance().getGame(cwl_id)
        part.flatMap(row => {
          val sellD2: Sell = hive_d2model(row, cwl_id)
          val sells: util.LinkedList[d3model.Sell] = TransD3Data.sell(ga, sellD2)
          val scala = sells.asScala
          scala
        })
      })
      val d3: RDD[D3_Sell] = value.map(sell => {
        D3_Sell(
          sell.getCs_issue_sell_fk,
          sell.getCs_issue_fk,
          sell.getCs_date_fk.toInt,
          sell.getCs_index,
          sell.getCs_ticket_code,
          sell.getCs_ticket_mark,
          sell.getCs_ticket_price,
          sell.getCs_total_price,
          sell.getCs_time_fk,
          sell.getCs_cp_fk,
          sell.getCs_s_fk,
          sell.getCs_multiple_fk,
          sell.getCs_pl_fk,
          sell.getCs_ticket_pl_fk,
          sell.getCs_t_offset_fk,
          StringUtils.substring(sell.getCs_issue_fk.toString, 3)
        )
      })
      //临时表名
      var tn = "a"+StringUtils.substringBefore(UUID.randomUUID.toString, "-")
//    d3.toDF().createTempView(tn)
      //将转换完的结果写出
//    if(cwl_id.equals("10001")) {
//      d3.toDF().coalesce(30).createTempView(tn)
//    }else{
//      d3.toDF().coalesce(5).createTempView(tn)
//    }

    d3.toDF().createTempView(tn)
    val dt = LocalDate.now().toString()
      ses.sql(
        s"""
           |INSERT OVERWRITE TABLE $d3_table PARTITION(etl_date='$etl_date',cwl_id='$cwl_id',issue) select * from $tn
           |""".stripMargin)
//      val res = ses.sql(
//        s"""
//           |select sum(cs_ticket_mark) as tickets,sum(cs_total_price) as total_price,sum(cs_ticket_price) as ticket_price from (SELECT * FROM olap.cs WHERE cwlid='$list_cwl_id'  and etl_date='$dt') as cs
//           |inner join olap.issue on issue.issue_pk = cs.cs_issue_fk
//           |inner join olap.cp on cp.cp_pk = cs.cs_cp_fk
//           |inner join olap.d on d.d_pk = cs.cs_date_fk
//           |inner join olap.s on s.s_pk = cs.cs_s_fk
//           |inner join olap.t on t.t_pk = cs.cs_time_fk
//           |inner join olap.m on m.m_pk = cs.cs_multiple_fk
//           |inner join olap.pl on pl.pl_pk = cs.cs_pl_fk
//           |left join olap.tpl on tpl.tpl_pk = cs.cs_ticket_pl_fk
//           |inner join olap.time_offset on time_offset.to_pk = cs.cs_to_fk
//           |""".stripMargin).collect().take(1)(0)


    ses.close()
    }



  def hive_d2model(row: Row, cwl_id: String): com.ilotterytech.ocean.dp.D1D2.d2model.Sell = {
    val play_type_map: Map[String, String] = row.getAs[Map[String, String]]("play_type_map")
    val bet_number_map: Map[String, String] = row.getAs[Map[String, String]]("bet_number_map")
    val times_map: Map[String, String] = row.getAs[Map[String, String]]("times_map")
    val total_num_map: Map[String, String] = row.getAs[Map[String, String]]("total_num_map")
    val total_cost_map: Map[String, String] = row.getAs[Map[String, String]]("total_cost_map")
    val keySet = play_type_map.keySet
    val infoes: Array[Sell.BetInfo] = new Array[Sell.BetInfo](keySet.size)
    for (elem <- keySet) {
      val info: Sell.BetInfo = Sell.BetInfo.builder().playType(play_type_map(elem))
        .betNumber(bet_number_map(elem))
        .times(times_map(elem).toInt)
        .totalNum(total_num_map(elem).toInt)
        .totalCost(total_cost_map(elem).toInt).build()
      infoes(elem.toInt) = info
    }
    val sell = Sell.builder()
      .id("id")
      .serial(row.getAs[String]("ticket_id"))
      .gameId(row.getAs[String]("cwl_id").toInt)
      .province(row.getAs[String]("province").toInt)
      .stationId(row.getAs[String]("station_id"))
      .sellIssue(row.getAs[String]("sell_issue"))
      .validIssue(row.getAs[String]("valid_issue"))
      .orderDatetime(row.getAs[String]("order_datetime"))
      .orderMethod(row.getAs[String]("order_method").toInt)
      .content("")
      .orderNum(row.getAs[String]("order_num").toLong)
      .totalCost(row.getAs[String]("total_cost").toLong)
      .cwlCode(cwl_id)
      .eltDate("")
      //销售终端机（投注机）编号，（一般情况下站点只有一台机器，而彩票信息中给的站点编号为 8位数字 ，此种情况时 站点编号+01 = 机器编号）
      .machineId(row.getAs[String]("station_id"))
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
                      cs_issue_sell_fk: Long,
                      cs_issue_fk: Long,
                      cs_date_fk: Int,
                      cs_index: Int,
                      cs_ticket_code: String,
                      cs_ticket_mark: Int,
                      cs_ticket_price: Long,
                      cs_total_price: String,
                      cs_time_fk: Int,
                      cs_cp_fk: Int,
                      cs_s_fk: Long,
                      cs_multiple_fk: Int,
                      cs_pl_fk: Int,
                      cs_ticket_pl_fk: String,
                      cs_to_fk: Int,
                      issue:String
                    )
}
