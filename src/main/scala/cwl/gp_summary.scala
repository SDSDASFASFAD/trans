package cwl

object gp_summary extends abstract_audit {
  def main(args: Array[String]): Unit = {

    val date:String = args(0)

    val special_char = "$"


//    import sparkSession.implicits._


//    queryGp("index_rpt_province_draw_day_sale").createTempView("index_rpt_province_draw_day_sale")


      queryGp(
        s"""
           |select
           |    gameid,
           |    case when gamename= '快乐8(新)' then '快乐8'
           |         when gamename= '3D'        then '福彩3D'
           |    else gamename end  as  gamename,
           |    concat('第',drawnumber,'期') as drawnumber,
           |    concat(case when provinceid = '40' then '50'
           |    else provinceid end ,'$special_char',provincename) as provinceid,
           |    sales
           |    from index_rpt_province_draw_day_sale
           |    where  gameid != '01000' and  gamename != '快乐8' and datadate = '$date'
           |    union all
           |    select
           |    distinct
           |    '64',
           |    gamename,
           |    concat('第',replace(datadate,'-',''),'期') as drawnumber,
           |    concat(case when provinceid = '40' then '50'
           |    else provinceid end ,'$special_char',provincename) as provinceid ,
           |    sales
           |    from index_rpt_province_draw_day_sale
           |    where gameid = '01000' and datadate = '$date'
           |    order by gamename,drawnumber,provinceid
           |""".stripMargin).createTempView("gp")


    sparkSession.sql(
      s"""
        |insert overwrite table test.gp_summary partition(cur_date='$date')
        |select * from gp
        |""".stripMargin)
//    summary
//      .write
//      .format("csv")
//      .option("header","false")
//      .option("encoding","utf-8")  //utf-8
//      .mode("overwrite")
////      .save(s"hdfs://10.1.60.1:8020/tmp/trans/$date")
//      .save(s"hdfs://cwl_nameservice:8020/test/gp_summary/cur_date=$date")





  }


}
