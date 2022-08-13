package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 17:54
  * @Description 连续累加
  */
object Exercise05 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("test05")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val t1DF: DataFrame = spark.read
      .schema("dt string,cn string,point int")
      .option("delimiter", "\t")
      .format("csv")
      .load("sparkSQL/data/problem/set01/Exercise05/data.txt")
    t1DF.show()
    t1DF.createOrReplaceTempView("t1")

    spark.sql(
      """
        |select
        |    cn
        |    ,dt
        |    ,sum(point) over(partition by cn order by dt asc)   as sumpoint
        |from t1
      """.stripMargin).show()

    spark.stop()
  }

}
