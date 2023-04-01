package fujian

import com.ilotterytech.ocean.dp.D1D2.d2model.Sell
import com.ilotterytech.ocean.dp.D1D2.sile.SiLeGameFactoryFactory
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import yunnan.USLD2Bac.D2_SELL

import scala.collection.mutable

object USLD2Bac {
  def main(args: Array[String]): Unit = {

    val etl_date=args(0)
    val cwl_id=args(1)
    val src_table = args(2)
    val tar_table = args(3)

    val sparkSession: SparkSession = SparkSession.builder()
      .master("yarn")
      .enableHiveSupport()
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nostrick")
      .getOrCreate()

    import sparkSession.implicits._

    val dataFrame: DataFrame = sparkSession.sql(s" select *  from $src_table  where etl_date = '$etl_date' and cwl_id = '$cwl_id' ")

    val sell_d2: Dataset[D2_SELL] = dataFrame.mapPartitions(part => {

      val sile = SiLeGameFactoryFactory.getInstance()

      part.map(line => {

        val factory = sile.getFactory(cwl_id)
        val categoryFactory = factory.getCategoryFactory

        var status = ""

        if (0 == line.getAs[String]("cancel_flag").toInt) {
          status = "0"
        } else {
          status = "1"
        }

        var bet_number: String = line.getAs[String]("bet_number")

        if(cwl_id.equals("90016") && line.getAs[String]("play_serial_number").equals("6") && bet_number.trim.substring(0,2).equals("20")){
          bet_number = bet_number.replace("^", "$")
        }

        val sell: Sell = com.ilotterytech.ocean.dp.D1D2.d2model.Sell.builder()
          .id("1")
          .serial(line.getAs[String]("serial"))
          .stationId(line.getAs[String]("agent_id"))
          .sellIssue(line.getAs[String]("sell_issue"))
          .validIssue(line.getAs[String]("valid_issue"))
          .stationOrder(Integer.parseInt(line.getAs[String]("serial_number")))
          .orderDatetime(line.getAs[String]("sale_time"))
          .cancelFlag(line.getAs[String]("cancel_flag"))
          .cancelTime(line.getAs[String]("cancel_time"))
          .operator(line.getAs[String]("operator_id"))
          .content(bet_number)
          .orderMethod(line.getAs[String]("bet_type").toInt)
          .printFlag(line.getAs[String]("print_flag"))
          .orderNum(line.getAs[String]("bet_total").toLong)
          .totalCost(line.getAs[String]("bet_total").toLong * 2)
          .machineId(line.getAs[String]("agent_id"))
          .province(35)
          .cwlId(cwl_id)
          .status(status)
          .eltDate(etl_date)
          .build()


        val decoSell = sile.decodeSell(sell, factory, categoryFactory)
        val infos = decoSell.getBetInfos
        // 玩法
        val palyTypeMap = new mutable.HashMap[String, String]()
        // 投注号码
        val betNumberMap = new mutable.HashMap[String, String]()
        // 倍数
        val timesMap = new mutable.HashMap[String, Int]()
        // 注数
        val totalNumMap = new mutable.HashMap[String, Int]()
        // 金额
        val totalCostMap = new mutable.HashMap[String, Int]()
        for (i <- 0 until infos.length) {
          palyTypeMap.put(i.toString, infos(i).getPlayType)
          betNumberMap.put(i.toString, infos(i).getBetNumber)
          timesMap.put(i.toString, infos(i).getTimes)
          totalNumMap.put(i.toString, infos(i).getTotalNum)
          totalCostMap.put(i.toString, infos(i).getTotalCost)
        }
        D2_SELL(sell.getId, sell.getSerial, sell.getProvince.toString, sell.getStationId, sell.getSellIssue, sell.getValidIssue, sell.getStationOrder,
          sell.getOrderDatetime, sell.getOrderMethod, sell.getOrderNum, sell.getTotalCost, sell.getMachineId, sell.getStatus, sell.getCancelTime,
          sell.getCancelFlag, sell.getPrintFlag, sell.getOperator, sell.getShop_serial, sell.getIssue_serial, sell.getDrawTime,
          palyTypeMap, betNumberMap, timesMap, totalNumMap, totalCostMap, cwl_id, line.getAs[String]("valid_issue"))
      })
    })


    sell_d2.toDF().createTempView("tmp")

//    sparkSession.sql("select * from tmp ").show(50)
    sparkSession.sql(s"insert overwrite table $tar_table partition ( etl_date = '$etl_date' , cwl_id ,issue ) " +
                              s"select * from tmp"  )


  }

}
