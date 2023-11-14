package cwl

import cn.hutool.core.date.DateUtil

object audit_gp_kylin extends abstract_audit {
  def main(args: Array[String]): Unit = {

    val date = args(0)

    val special_char = "$"

    import sparkSession.implicits._


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

    val kylin_date = DateUtil.format(DateUtil.parseDate(date), "yyyy年MM月dd日")

    queryKylin(
      s"""
         |select
         |issue.issue_name,
         |s.s_province,
         |sum(cs_ticket_mark) as tickets,
         |sum(cs_total_price) as total_price,
         |sum(cs_ticket_price) as ticket_price
         |from cs
         |inner join d3.issue on issue.issue_pk = cs.cs_issue_fk
         |inner join d3.s on s.s_pk = cs.cs_s_fk
         |where issue_dayofyear = '$kylin_date' and   issue_playmode != '即开型-即开票'
         |group by issue.issue_name,s.s_province
         |order by issue.issue_name,s.s_province
         |""".stripMargin).createTempView("kylin_usl")

    queryKylin(
      s"""
         |select
         |issue.issue_dayofyear as issue_name,
         |s.s_province,
         |sum(cs_ticket_mark) as tickets,
         |sum(cs_total_price) as total_price,
         |sum(cs_ticket_price) as ticket_price
         |from cs
         |inner join d3.issue on issue.issue_pk = cs.cs_issue_fk
         |inner join d3.s on s.s_pk = cs.cs_s_fk
         |where issue_dayofyear = '$kylin_date' and   issue_playmode = '即开型-即开票'
         |group by issue.issue_dayofyear,s.s_province
         |""".stripMargin).createTempView("kylin_jkp")


    val kylin_query = sparkSession.sql(
      """
        |select
        |issue_name,
        |s_province,
        |tickets,
        |total_price,
        |ticket_price
        |from
        |(
        |select * from kylin_usl
        |union all
        |select * from kylin_jkp
        |)
        |""".stripMargin)

//    kylin_query.show(1000)

    kylin_query.map(row=>{

      var game_name = ""
      var issue = ""

      val issue_name:java.lang.String = row.getAs[java.lang.String]("issue_name")
      if(!issue_name.contains("第")){
        game_name = "即开票"
        issue = "第"+ issue_name.replace("年","").replace("月","").replace("日","")+"期"
      }else {
        val strings = issue_name.replace("第", "\t第").split("\t")
        game_name=strings(0)
        issue=strings(1)
      }


      val province = row.getAs[String]("s_province")
      val tickets = row.getAs[Long]("tickets")
      val total_price = row.getAs[Long]("total_price")
      val ticket_price = row.getAs[Long]("ticket_price")

      kylin(game_name,issue,province,tickets,new java.math.BigDecimal(total_price) ,new java.math.BigDecimal(ticket_price))
    }).createTempView("kylin")

    sparkSession.sql(
      s"""
        |insert overwrite table test.kylin_summary partition(cur_date='$date')
        |select * from kylin
        |""".stripMargin)

    val gp_kylin_audit = sparkSession.sql(
      s"""
        | select
        | gamename,
        | drawnumber,
        | provinceid,
        | sales,
        | kylin_game_name,
        | kylin_draw_number,
        | kylin_province,
        | kylin_tickets,
        | kylin_total_price,
        | kylin_ticket_price,
        | if(sales = kylin_ticket_price,'true','false') as result
        | from  gp
        | left join kylin
        | on gp.gamename=kylin.kylin_game_name and gp.drawnumber = kylin.kylin_draw_number and gp.provinceid = kylin.kylin_province
        | order by gamename,drawnumber,provinceid
        |""".stripMargin)

    gp_kylin_audit.createTempView("gp_kylin_audit")

    gp_kylin_audit.show(100000)

    sparkSession.sql(
      """
        |select * from gp_kylin_audit where result = 'false' order by gamename,drawnumber,provinceid
        |""".stripMargin).show(100000)

    gp_kylin_audit
      .coalesce(1)
      .write
          .format("csv")
          .option("header","false")
          .option("encoding","gbk")  //utf-8
          .mode("overwrite")
          .save(s"hdfs://cwl-nameservice/test/gp_kylin_audit/main_$date")


  }

  case class gp_summary(
                         gp_cwl_id:String,
                         gp_game_name:String,
                         gp_draw_number:String,
                         gp_province:String,
                         gp_sale_summary:java.math.BigDecimal
                       )

  case class kylin(
                  kylin_game_name:String,
                  kylin_draw_number:String,
                  kylin_province:String,
                  kylin_tickets:Long,
                  kylin_total_price:java.math.BigDecimal,
                  kylin_ticket_price:java.math.BigDecimal
                  )
}
