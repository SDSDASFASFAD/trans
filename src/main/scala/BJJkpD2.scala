import java.util

import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.JavaConverters.asScalaBufferConverter

object BJJkpD2 {
  def main(args: Array[String]): Unit = {
    val cwl_id=args(0)
    val dt= args(1)
    val ses = SparkSession.builder()
      //      .master("local[*]")
      .enableHiveSupport()
      //      .config("spark.shuffle.service.enabled", "true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("hive.exec.dynamic.partition", "true")
      .config("hive.exec.dynamic.partition.mode", "nostrick")
      .getOrCreate()
    import ses.implicits._

    val dataFrame = ses.sql(
      s"""
        | select
        | concat(jkp_sales.BATCHNUMBER, "_", jkp_sales.UNIT) as jkp_code,
        | jkp_sales.FROMTYPE as jkp_from_type,
        | jkp_sales.fromtratrm as jkp_from_station,
        |	jkp_sales.FROMTRATRM as jkp_from_machine,
        | jkp_sales.TOTYPE as jkp_to_type,
        | jkp_sales.TOTRAAGT as jkp_to_station,
        | jkp_sales.TOTRATRM as jkp_to_machine,
        |	jkp_sales.SETTLETYPE as jkp_sold_type,
        | s.STARTDATE as jkp_start_date,
        | s.ENDDATE as jkp_end_date,
        | jkp_sales.TRATIME as jkp_date,
        |	jkp_sales.BATCHNUMBER as jkp_serial,
        | concat("即开型", "-", s.GAMENAME) as jkp_game,
        | jkp_sales.WAGERAMT as jkp_total_cost,
        |	ipsgamesize.PIECESINUNIT as piecesinunit ,
        | ipsgamesize.PRICE price,
        | jkp_sales.gamenumber as game_number
        |	from (
        | select
        |   *
        |   from
        |   ticket.jkp_sales_tmp
        |   ) as jkp_sales
        |	inner join (select a.traprod,b.gamename,a.gamecode,a.gamebatch,b.price,b.gametypename,a.gametypecode,a.startdate,a.enddate,a.status,a.ordernum
        |	from ticket.ipsgame a join ticket.ipsgame_sqlserver b  on a.traprod=b.traprod) s on jkp_sales.PRODUCT =s.TRAPROD
        |	inner join ticket.ipsgamesize on jkp_sales.PRODUCT =ipsgamesize.TRAPROD
        |""".stripMargin)
    //    val filter = dataFrame.filter(row => row.getAs[String]("price").toDouble * row.getAs[String]("piecesinunit").toInt != row.getAs[String]("jkp_total_cost").toDouble)

    val value: Dataset[String] = dataFrame.flatMap(row => {
      val list = new util.LinkedList[String]
      val jkp_code = row.getAs[String]("jkp_code")
      val jkp_from_type = row.getAs[String]("jkp_from_type")
      val jkp_from_station = row.getAs[String]("jkp_from_station")
      val jkp_from_machine = row.getAs[String]("jkp_from_machine")
      val jkp_to_type = row.getAs[String]("jkp_to_type")
      val jkp_to_station = row.getAs[String]("jkp_to_station")
      val jkp_to_machine = row.getAs[String]("jkp_to_machine")
      val jkp_sold_type = row.getAs[String]("jkp_sold_type")
      val jkp_start_date = row.getAs[String]("jkp_start_date")
      val jkp_end_date = row.getAs[String]("jkp_end_date")
      val jkp_date = row.getAs[String]("jkp_date")
      val jkp_serial = row.getAs[String]("jkp_serial")
      val jkp_game = row.getAs[String]("jkp_game")
      val jkp_total_cost = row.getAs[String]("jkp_total_cost")
      val piecesinunit = row.getAs[String]("piecesinunit")
      val price = row.getAs[String]("price")
      val game_number = row.getAs[String]("game_number")
      val etl_date =
      for (i <- 1 to piecesinunit.toInt) {
        val jkpCode = jkp_code + "_" + i

        val jkpLine = jkpCode + "\t" + jkp_from_type + "\t" + jkp_from_station + "\t" +
          jkp_from_machine + "\t" + jkp_to_type + "\t" + jkp_to_station + "\t" +
          jkp_to_machine + "\t" + jkp_sold_type + "\t" + jkp_start_date + "\t" +
          jkp_end_date + "\t" + jkp_date + "\t" + jkp_serial + "\t" +
          game_number + "\t" + price
        list.add(jkpLine)
      }
      val scala = list.asScala
      scala
    })
    val d2_jkp = value.map(line => {
      val split = line.split("\t")
      Jkp_d2(split(0), split(1), split(2), split(3)
        , split(4), split(5), split(6), split(7), split(8)
        , split(9), split(10), split(11), split(12), split(13))
    })
    d2_jkp.toDF().createTempView("tmp")
//    val dt = LocalDate.now().toString
    //    ses.sql("select * from tmp").show(101)
    ses.sql(
      s"""
         |INSERT OVERWRITE TABLE audit_d2.jkp PARTITION(dt='$dt') select * from tmp
         |""".stripMargin)

    ses.stop()
  }
  case class Jkp_d2(
                     jkp_code:String,
                     jkp_from_type:String,
                     jkp_from_station:String,
                     jkp_from_machine:String,
                     jkp_to_type:String,
                     jkp_to_station:String,
                     jkp_to_machine:String,
                     jkp_sold_type:String,
                     jkp_start_date:String,
                     jkp_end_date:String,
                     jkp_date:String,
                     jkp_serial:String,
                     jkp_game:String,
                     jkp_cost:String
                   )
}
