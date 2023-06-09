import com.ilotterytech.ocean.dp.D1D2.beans.LotteryConsume
import com.ilotterytech.ocean.dp.D1D2.suicai.SuicaiGameFactoryFactory
import com.ilotterytech.ocean.dp.D1D2.suicai.utils.ScTickerUtils
import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.collection.mutable

object test {
  def main(args: Array[String]): Unit = {


    val ses = SparkSession
      .builder()
      .appName("")
      .master("local[*]")
      .getOrCreate()
    import ses.implicits._


    val selldata: DataFrame = ses.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "false") //是否自动推到内容的类型
      .option("delimiter", ",") //分隔符，默认为 ,
      .load("file\\3d.csv")

    selldata.show()

    selldata.collect().foreach(println)

    val d2_data: Dataset[D2_SELL] = selldata.mapPartitions(partition => {
      val gameFactory = SuicaiGameFactoryFactory.getInstance().getFactory("10002")
      val factory = gameFactory.getCategoryFactory()
      partition.flatMap(row => {

        val palyTypeMap = new mutable.HashMap[String, String]()
        val betNumberMap = new mutable.HashMap[String, String]()
        val timesMap = new mutable.HashMap[String, Int]()
        val totalNumMap = new mutable.HashMap[String, Int]()
        val totalCostMap = new mutable.HashMap[String, Int]()

        var brd1: String = ""
        if (!row.getAs[String]("brd1_1").equals("null")) {
          brd1 = "bettyp:" + row.getAs[String]("brd1_1") + ";multiple:" + row.getAs[String]("brd1_2") + ";system:" + row.getAs[String]("brd1_3") + ";board:" + row.getAs[String]("brd1_4")
        }
        var brd2: String = ""
        if (!row.getAs[String]("brd2_1").equals("null")) {
          brd2 = "bettyp:" + row.getAs[String]("brd2_1") + ";multiple:" + row.getAs[String]("brd2_2") + ";system:" + row.getAs[String]("brd2_3") + ";board:" + row.getAs[String]("brd2_4")
        }

        var brd3: String = ""
        if (!row.getAs[String]("brd3_1").equals("null")) {
          brd3 = "bettyp:" + row.getAs[String]("brd3_1") + ";multiple:" + row.getAs[String]("brd3_2") + ";system:" + row.getAs[String]("brd3_3") + ";board:" + row.getAs[String]("brd3_4")
        }
        var brd4: String = ""
        if (!row.getAs[String]("brd4_1").equals("null")) {
          brd4 = "bettyp:" + row.getAs[String]("brd4_1") + ";multiple:" + row.getAs[String]("brd4_2") + ";system:" + row.getAs[String]("brd4_3") + ";board:" + row.getAs[String]("brd4_4")
        }
        var brd5: String = ""
        if (!row.getAs[String]("brd5_1").equals("null")) {
          brd5 = "bettyp:" + row.getAs[String]("brd5_1") + ";multiple:" + row.getAs[String]("brd5_2") + ";system:" + row.getAs[String]("brd5_3") + ";board:" + row.getAs[String]("brd5_4")
        }

        val content: String = ScTickerUtils.appendBrd(brd1, brd2, brd3, brd4, brd5)
        val contens = factory.decodeContent(content)
        for (i <- 0 until contens.length) {
          val prefix = factory.decodeCategoryPrefix(contens(i))
          val decoder = factory.getCategoryDecoder(prefix)
          println("**********prefix********"+prefix)
          println("************contens**********"+contens(i))
          val consume = decoder.decode(contens(i), new LotteryConsume)
          palyTypeMap.put(i.toString, consume.getCategoryId.toString)
          betNumberMap.put(i.toString, consume.getPrefixNumber)
          timesMap.put(i.toString, consume.getTimes)
          totalNumMap.put(i.toString, consume.getTotalNum)
          totalCostMap.put(i.toString, consume.getTotalPrice)
        }

        var dsells: Array[D2_SELL] = null;
        if ("pck3".equals("pck3")) {
          val draws: Int = row.getAs[Integer]("draws").toInt
          dsells = D1toD2_S3(row, draws, palyTypeMap, betNumberMap, timesMap, totalNumMap, totalCostMap)
        } else {
          dsells = D1toD2(row, palyTypeMap, betNumberMap, timesMap, totalNumMap, totalCostMap)
        }
        dsells
      })
    })
//    d2_data.toDF().createTempView("tmp")

    d2_data.collect().foreach(println)
//    val dt = LocalDate.now().toString
    //    ses.sql(
//      s"""
//         |INSERT OVERWRITE TABLE bjd1d2d3.d2_sell PARTITION(dt='$dt',cwl_id='$cwl_id') select * from tmp
//         |""".stripMargin)
  }

  def D1toD2(row:Row,
             palyTypeMap:mutable.HashMap[String, String],
             betNumberMap:mutable.HashMap[String, String],
             timesMap:mutable.HashMap[String, Int],
             totalNumMap:mutable.HashMap[String, Int],
             totalCostMap:mutable.HashMap[String, Int]): Array[D2_SELL] ={
    val id: String = DigestUtils.md5Hex(row.toString())
    val game_id = 0
    //票号
    val serial = row.getAs[String]("trasn")
    //站点编号
    val station_id = row.getAs[String]("traagt")
    //期号
    val sell_issue = row.getAs[String]("indraw")

    val valid_issue = row.getAs[String]("indraw")

    val station_order = 0
    // 销售时间
    val order_datetime = row.getAs[String]("tratime")
    //  投注方式
    val order_method = row.getAs[Integer]("entry")
    //  投注注数
    val order_num = row.getAs[Float]("traamt").toInt/2
    //  投注金额
    val total_cost = row.getAs[Float]("traamt").toInt
    //  投注机id
    val machine_id = row.getAs[String]("traagt") + "01"
    //  票状态
    val status = row.getAs[Int]("iscancel")
    //  注销时间
    val cancel_time = ""
    //  是否手工作废
    val cancel_flag = row.getAs[Int]("ismancan")

    val print_flag = "0"
    val operator = ""
    val shop_serial = ""
    val issue_serial = ""
    val draw_time = ""

    Array(D2_SELL(id, serial,
      game_id, station_id,
      sell_issue, valid_issue.toString,
      station_order, order_datetime,
      order_method, order_num,
      total_cost, machine_id,
      status.toString, cancel_time, cancel_flag.toString,
      print_flag, operator, shop_serial,
      issue_serial
      , draw_time, palyTypeMap, betNumberMap, timesMap, totalNumMap, totalCostMap))
  }

  def D1toD2_S3(row:Row,
                draw:Int,
                palyTypeMap:mutable.HashMap[String, String],
                betNumberMap:mutable.HashMap[String, String],
                timesMap:mutable.HashMap[String, Int],
                totalNumMap:mutable.HashMap[String, Int],
                totalCostMap:mutable.HashMap[String, Int]):Array[D2_SELL]={

    val id: String = DigestUtils.md5Hex(row.toString())
    val game_id = 0
    //票号
    val serial = row.getAs[String]("trasn")
    //站点编号
    val station_id = row.getAs[String]("traagt")
    //期号
    var sell_issue = row.getAs[String]("indraw")

    var externalDraw = Integer.valueOf(sell_issue)
    val station_order = 0
    val order_datetime = row.getAs[String]("tratime")
    val order_method = row.getAs[Integer]("entry")

    val machine_id = row.getAs[String]("traagt") + "01"
    val status = row.getAs[Int]("iscancel")
    val cancel_time = ""
    val cancel_flag = row.getAs[Int]("ismancan")
    val print_flag = "0"
    val operator = ""
    val shop_serial = ""
    val issue_serial = ""
    val draw_time = ""

    val traamt: Float = row.getAs[Float]("traamt")

    val res = new Array[D2_SELL](draw)

    for (i <- 0 until draw) {

      val valid_issue = externalDraw+i

      val total_cost = traamt.toInt/draw

      val order_num = total_cost.toInt / 2

      val dsell: D2_SELL = D2_SELL(id, serial,
        game_id, station_id,
        sell_issue, valid_issue.toString,
        station_order, order_datetime,
        order_method, order_num,
        total_cost, machine_id,
        status.toString, cancel_time, cancel_flag.toString,
        print_flag, operator, shop_serial,
        issue_serial
        , draw_time, palyTypeMap, betNumberMap, timesMap, totalNumMap, totalCostMap)

      res(i)=dsell
    }
    res
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

  case class ticket_schema(
                            trasn:String,
                            tratime:String,
                            traagt:String,
                            traprod:String,
                            entry:Int,
                            qp:Int,
                            pia:Int,
                            kenomultiplier:Int,
                            nbrbrds:Int,
                            indraw:String,
                            draws:Int,
                            brd1_1:String,
                            brd1_2:String,
                            brd1_3:String,
                            brd1_4:String,
                            brd2_1:String,
                            brd2_2:String,
                            brd2_3:String,
                            brd2_4:String,
                            brd3_1:String,
                            brd3_2:String,
                            brd3_3:String,
                            brd3_4:String,
                            brd4_1:String,
                            brd4_2:String,
                            brd4_3:String,
                            brd4_4:String,
                            brd5_1:String,
                            brd5_2:String,
                            brd5_3:String,
                            brd5_4:String,
                            traamt:Float,
                            iscancel:Int,
                            ismancan:Int
                          )

}
