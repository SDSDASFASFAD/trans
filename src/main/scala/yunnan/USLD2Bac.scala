package yunnan

import cn.hutool.core.date.DateUtil
import com.ilotterytech.ocean.dp.D1D2.sile.SiLeGameFactoryFactory
import org.apache.spark.sql.SparkSession

import scala.collection.mutable;

object USLD2Bac {
  def main(args: Array[String]): Unit = {
    val cwl_id=args(0)
//    val start_issue=args(1)
//    val end_issue=args(2)
    val ods_table=args(3)
    val d2_table=args(4)
    val etl_date=args(5)
//    val cwl_id="10003"
//    val start_issue="2022001"
//    val end_issue="2022001"
//    val ods_table="test.yn_sales"
//    val d2_table="test.yn_d2_sales"
//    val etl_date="2023-02-03"
    val ses = SparkSession.builder()
      .master("yarn")
      .enableHiveSupport()
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nostrick")
      .getOrCreate()
    import ses.implicits._
//    val raw_data = ses.sql(
//      s"""
//        |select stationid,sales_issue,valid_issue,sales_time,ordermethod,content,ordernum,cwl_id from
//        |$ods_table where cwl_id=$cwl_id and issue>='$start_issue' and issue<='$end_issue'
//        |""".stripMargin)

    val raw_data = ses.sql(
      s"""
         |select stationid,sales_issue,valid_issue,sales_time,ordermethod,content,ordernum,cwl_id from
         |$ods_table where etl_date='$etl_date' and  cwl_id='$cwl_id'
         |""".stripMargin)
    val d2Data = raw_data.mapPartitions(part => {

      val sile = SiLeGameFactoryFactory.getInstance()
      part.map(line => {
        val factory = sile.getFactory(line.getAs[String]("cwl_id"))
        val categoryFactory = factory.getCategoryFactory

        var sell_time = "" ;
        if (ods_table.equals("ticket.sales_1")){
             sell_time= DateUtil.format(DateUtil.parseDateTime(line.getAs[String]("sales_time")),"yyyy-MM-dd HH:mm:ss")
        }else{
          sell_time = line.getAs[String]("sales_time")
        }


        val sell = com.ilotterytech.ocean.dp.D1D2.d2model.Sell.builder()
          .id("1")
          .serial("1")
          .gameId(0)
          .province(53)
          .stationId(line.getAs[String]("stationid"))
          .sellIssue(line.getAs[String]("sales_issue"))
          .validIssue(line.getAs[String]("valid_issue"))
          .orderDatetime(sell_time)
          .orderMethod(Integer.parseInt(line.getAs[String]("ordermethod")))
          .content(line.getAs[String]("content"))
          .orderNum(line.getAs[String]("ordernum").toLong)
          .totalCost(line.getAs[String]("ordernum").toLong * 2)
          .cwlCode(cwl_id)
          .eltDate("")
          .machineId(line.getAs[String]("stationid"))
          .status("0")
          .build()

        val decoSell = sile.decodeSell(sell, factory, categoryFactory)
        val infos = decoSell.getBetInfos
        val palyTypeMap = new mutable.HashMap[String, String]()
        val betNumberMap = new mutable.HashMap[String, String]()
        val timesMap = new mutable.HashMap[String, Int]()
        val totalNumMap = new mutable.HashMap[String, Int]()
        val totalCostMap = new mutable.HashMap[String, Int]()
        for (i <- 0 until infos.length) {
          palyTypeMap.put(i.toString, infos(i).getPlayType)
          betNumberMap.put(i.toString, infos(i).getBetNumber)
          timesMap.put(i.toString, infos(i).getTimes)
          totalNumMap.put(i.toString, infos(i).getTotalNum)
          totalCostMap.put(i.toString, infos(i).getTotalCost)
        }
        D2_SELL(sell.getId, sell.getSerial, sell.getProvince.toString,sell.getStationId, sell.getSellIssue, sell.getValidIssue, sell.getStationOrder,
          sell.getOrderDatetime, sell.getOrderMethod, sell.getOrderNum, sell.getTotalCost, sell.getMachineId, sell.getStatus, sell.getCancelTime,
          sell.getCancelFlag, sell.getPrintFlag, sell.getOperator, sell.getShop_serial, sell.getIssue_serial, sell.getDrawTime,
          palyTypeMap, betNumberMap, timesMap, totalNumMap, totalCostMap,line.getAs[String]("cwl_id"),line.getAs[String]("valid_issue"))
      })
    })
//    d2Data.toDF().show()
//    if(cwlid.equals("10001")) {
//      d2Data.toDF().coalesce(30).createTempView("tmp")
//    }else{
//      d2Data.toDF().coalesce(5).createTempView("tmp")
//    }
    d2Data.toDF().createTempView("tmp")
    ses.sql(s"INSERT OVERWRITE TABLE $d2_table PARTITION(etl_date='$etl_date',cwl_id,issue) select * from tmp")

  }
  case class D2_SELL(
                      raw_hash: String,
                      ticket_id: String,
                      province:String,
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
                      play_type_map: mutable.Map[String, String],
                      bet_number_map: mutable.Map[String, String],
                      times_map: mutable.Map[String, Int],
                      total_num_map: mutable.Map[String, Int],
                      total_cost_map: mutable.Map[String, Int],
                      cwl_id:String,
                      issue:String
                    )
}
