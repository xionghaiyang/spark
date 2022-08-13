package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 19:50
  * @Description 有用户每日游戏时长数据
  *              1)统计每个用户每天游戏累计时长。(要求同一用户每天游戏时长累加之前所有天的游戏时长）
  *              2)统计每个用户每天游戏时长累加前一天游戏时长的累计时长。
  *              3)统计每个用户每天游戏时长累加后一天游戏时长的累计时长。
  */
object Exercise06 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Exercise06")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val t2DF: DataFrame = spark.read
      .schema("uid string,dt string,duration int")
      .option("header", "true")
      .option("delimiter", ",")
      .csv("sparkSQL/data/problem/set01/Exercise06/UserPlayData.txt")
    t2DF.show()
    t2DF.createOrReplaceTempView("t2")

    spark.sql(
      """
        |select
        |    uid
        |    ,dt
        |    ,sum(duration) over(partition by uid order by dt asc)   as total
        |from t2
      """.stripMargin).show()

    spark.sql(
      """
        |select
        |    uid
        |    ,dt
        |    ,sum(duration) over(partition by uid order by dt asc rows between 1 preceding and current row)   as total
        |from t2
      """.stripMargin).show()

    spark.sql(
      """
        |select
        |    uid
        |    ,dt
        |    ,sum(duration) over(partition by uid order by dt asc rows between current row and 1 following)   as total
        |from t2
      """.stripMargin).show()

    spark.stop()
  }

}
