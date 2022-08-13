package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 20:11
  * @Description 行列变换操作
  */
object Exercise09 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Exercise09")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val aDF: DataFrame = spark.read
      .option("header", "true")
      .schema("username string,item string,price int")
      .option("delimiter", ",")
      .csv("sparkSQL/data/problem/set01/Exercise09/row-col.txt")
    aDF.show()
    aDF.createOrReplaceTempView("a")

    val bDF: DataFrame = spark.sql(
      """
        |select
        |    username
        |    ,max(case when item = 'A' then price else 0 end)    as  A
        |    ,max(case when item = 'B' then price else 0 end)    as  B
        |    ,max(case when item = 'C' then price else 0 end)    as  C
        |    ,max(case when item = 'D' then price else 0 end)    as  D
        |from a
        |group by
        |    username
      """.stripMargin)
    bDF.show()
    bDF.createOrReplaceTempView("b")

    spark.sql(
      """
        |select
        |    username
        |    ,'A'    as  item
        |    ,A  as  price
        |from b
        |where A <> 0
        |union
        |select
        |    username
        |    ,'B' as item
        |    ,B  as  price
        |from b
        |where B <> 0
        |union
        |select
        |    username
        |    ,'C' as item
        |    ,C  as  price
        |from b
        |where C <> 0
        |union
        |select
        |    username
        |    ,'D' as item
        |    ,D  as  price
        |from b
        |where D <> 0
      """.stripMargin).show()

    spark.sql(
      """
        |select
        |    t.username
        |    ,nvl(t.mp['A'],0)   as  A
        |    ,nvl(t.mp['B'],0)   as  B
        |    ,nvl(t.mp['C'],0)   as  C
        |    ,nvl(t.mp['D'],0)   as  D
        |from (
        |    select
        |        username
        |        ,str_to_map(concat_ws(',',collect_list(concat(item,':',price))),',',':')    as  mp
        |    from a
        |    group by
        |        username
        |) t
      """.stripMargin).show()

    spark.sql(
      """
        |select
        |    t.username
        |    ,t.item
        |    ,t.price
        |from (
        |    select
        |        username
        |        ,item
        |        ,price
        |    from b
        |    lateral view explode(map('A',A,'B',B,'C',C,'D',D))  subview  as  item,price
        |) t
        |where t.price <> 0
      """.stripMargin).show()

    spark.stop()
  }

}
