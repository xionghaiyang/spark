package com.sean.sparksql.problem.set01

import org.apache.spark.sql.SparkSession

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 17:28
  * @Description 有一个拉链表：存款利率表 bal_rat_table
  *              id：用户id 、bal:用户存款、rate:利率、startDate:开始时间、endDate:结束日期，9999代表不知道何时结束。
  *              需求:拉链表中存在多种状态的数据（存款），开始日期和结束日期不一致，
  *              求出2019年第一个季度：2019-01-01至2019-03-31的每个人的利息。
  */
object Exercise03 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Exercise03")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.read
      .schema("id int,bal int,rate double,startdate string,enddate string")
      .option("delimiter", "\t")
      .format("csv")
      .load("./sparkSQL/data/problem/set01/Exercise03/bal_rat_table.txt")
      .createOrReplaceTempView("bal_rat_table")

    spark.sql("select * from bal_rat_table").show()

    spark.sql(
      """
        |select
        |    id
        |    ,sum(bal * rate * datediff(case when enddate > '2019-03-31' then '2019-03-01' else enddate end,case when startdate < '2019-01-01' then '2019-01-01' else startdate end))    as  total_sum
        |from bal_rat_table
        |where startdate < '2019-03-31' and enddate > '2019-01-01'
        |group by
        |    id
      """.stripMargin).show()

    spark.stop()
  }

}
