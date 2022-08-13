package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 21:04
  * @Description 行列变换操作
  */
object Exercise10 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Exercise10")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val aDF: DataFrame = spark.read
      .schema("username string,item string,price int")
      .option("header", "true")
      .option("delimiter", ",")
      .csv("sparkSQL/data/problem/set01/Exercise10/row-col.txt")
    aDF.show()
    aDF.createOrReplaceTempView("a")

    val bDF: DataFrame = spark.sql(
      """
        |select
        |    username
        |    ,concat_ws(',',collect_list(item))  as  item
        |    ,sum(price) as  price
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
        |    ,subview.item
        |from b
        |lateral view explode(split(item,','))   subview as  item
      """.stripMargin).show()

    spark.stop()
  }

}
