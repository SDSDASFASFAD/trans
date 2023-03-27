import com.ilotterytech.ocean.dp.D1D2.beans.LotteryConsume
import com.ilotterytech.ocean.dp.D1D2.suicai.SuicaiGameFactoryFactory
import com.ilotterytech.ocean.dp.D1D2.suicai.utils.ScTickerUtils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import java.sql.Timestamp

import com.ilotterytech.ocean.dp.D1D2.interfaces.GameFactory

import scala.collection.mutable

object NewBjD1toD2_test {
  def main(args: Array[String]): Unit = {
    val ses = SparkSession
      .builder()
      .appName("")
      .master("yarn")
      .enableHiveSupport()
      .getOrCreate()
    import ses.implicits._


    val selldata = ses.sql(
      """
        |select * from test.zh_test1
        |""".stripMargin)
    val D2_data: Dataset[D2_SELL] = selldata.mapPartitions(partition => {
      val gameFactory = SuicaiGameFactoryFactory.getInstance().getFactory("10001")
      val factory = gameFactory.getCategoryFactory
      partition.map(row => {
        // getas 方法可以直接从DataFrame中获取该属性的列
        val md5: String = DigestUtils.md5Hex(row.toString())
        val gameId = factory.getGameFactory.getGameId
        val serial: String = row.getAs[String]("trasn")
        val station_id: String = row.getAs[String]("traagt")
        val sell_issue = ""
        val valid_issue = ""
        val station_order = 0
        val order_datetime: Timestamp = row.getAs[Timestamp]("tratime")
        val order_method: String = row.getAs[String]("entry")
        val order_num = row.getAs[Double]("traamt").toInt / 2
        val total_cost = row.getAs[Double]("traamt").toInt
        val machine_id = row.getAs[String]("traagt") + "01"
        val status = row.getAs[String]("iscancel")
        val cancel_time = ""
        val cancel_flag = row.getAs[String]("ismancan")
        val print_flag = "0"
        val operator = ""
        val shop_serial = ""
        val issue_serial = ""
        val draw_time = ""
        val palyTypeMap = new mutable.HashMap[String, String]()
        val betNumberMap = new mutable.HashMap[String, String]()
        val timesMap = new mutable.HashMap[String, Int]()
        val totalNumMap = new mutable.HashMap[String, Int]()
        val totalCostMap = new mutable.HashMap[String, Int]()

        var brd1: String = " "
        if (row.getAs[String]("brd1bettype") != "") {
          brd1 = "bettyp:" + row.getAs[String]("brd1bettype") + ";multiple:" + row.getAs[String]("brd1multiple") + ";system:" + row.getAs[String]("brd1system") + ";board:" + row.getAs[String]("brd1board")
        }
        var brd2: String = " "
        if (row.getAs[String]("brd2bettype") != "") {
          brd2 = "bettyp:" + row.getAs[String]("brd2bettype") + ";multiple:" + row.getAs[String]("brd2multiple") + ";system:" + row.getAs[String]("brd2system") + ";board:" + row.getAs[String]("brd2board")
        }

        var brd3: String = " "
        if (row.getAs[String]("brd3bettype") != "") {
          brd3 = "bettyp:" + row.getAs[String]("brd3bettype") + ";multiple:" + row.getAs[String]("brd3multiple") + ";system:" + row.getAs[String]("brd3system") + ";board:" + row.getAs[String]("brd3board")
        }
        var brd4: String = " "
        if (row.getAs[String]("brd4bettype") != "") {
          brd4 = "bettyp:" + row.getAs[String]("brd4bettype") + ";multiple:" + row.getAs[String]("brd4multiple") + ";system:" + row.getAs[String]("brd4system") + ";board:" + row.getAs[String]("brd4board")
        }
        var brd5: String = " "
        if (row.getAs[String]("brd5bettype") != "") {
          brd5 = "bettyp:" + row.getAs[String]("brd5bettype") + ";multiple:" + row.getAs[String]("brd5multiple") + ";system:" + row.getAs[String]("brd5system") + ";board:" + row.getAs[String]("brd5board")
        }
        val traamt: Double = row.getAs[Double]("traamt")
        val iscancel: String = row.getAs[String]("iscancel")
        val ismancan: String = row.getAs[String]("ismancan")


        val content = ScTickerUtils.appendBrd(brd1, brd2, brd3, brd4, brd5)
        val contens = factory.decodeContent(content)
        for (i <- 0 until contens.length) {
          val prefix = factory.decodeCategoryPrefix(contens(i))
          val decoder = factory.getCategoryDecoder(prefix)
          val consume = decoder.decode(contens(i), new LotteryConsume)
          palyTypeMap.put(i.toString, consume.getCategoryId.toString)
          betNumberMap.put(i.toString, consume.getPrefixNumber)
          timesMap.put(i.toString, consume.getTimes)
          totalNumMap.put(i.toString, consume.getTotalNum)
          totalCostMap.put(i.toString, consume.getTotalPrice)
        }
        D2_SELL(md5, serial,
          gameId, station_id,
          sell_issue, valid_issue,
          station_order, order_datetime.toString,
          order_method.toInt, order_num,
          total_cost, machine_id,
          status.toString, cancel_time, cancel_flag.toString,
          print_flag, operator, shop_serial,
          issue_serial
          , draw_time, palyTypeMap, betNumberMap, timesMap, totalNumMap, totalCostMap)

      })
    })
    D2_data.toDF().createTempView("tmp")
    ses.sql(
      """
        |insert into table test.zh_d2sell
        |select * from tmp
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
                      play_type_map:mutable.Map[String,String],
                      bet_number_map:mutable.Map[String,String],
                      times_map:mutable.Map[String,Int],
                      total_num_map:mutable.Map[String,Int],
                      total_cost_map:mutable.Map[String,Int]
                    )
}
