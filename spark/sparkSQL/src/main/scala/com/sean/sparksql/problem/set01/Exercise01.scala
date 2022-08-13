package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 16:56
  * @Description 使用SQL查询a，b表中不相交的数据集。
  */
object Exercise01 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Exercise01")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val aDF: DataFrame = spark.read
      .schema("id int,name string")
      .option("delimiter", "\t")
      .format("csv")
      .load("./sparkSQL/data/problem/set01/Exercise01/a.txt")
    aDF.show()
    aDF.createOrReplaceTempView("a")

    val bDF: DataFrame = spark.read
      .schema("id int,name string")
      .option("delimiter", "\t")
      .format("csv")
      .load("./sparkSQL/data/problem/set01/Exercise01/b.txt")
    bDF.show()
    bDF.createOrReplaceTempView("b")

    spark.sql(
      """
        |select
        |    a.id
        |    ,a.name
        |from a
        |left join b on a.id = b.id
        |where b.id is null
        |union
        |select
        |    b.id
        |    ,b.name
        |from b
        |left join a on b.id = a.id
        |where a.id is null
      """.stripMargin).show()

    spark.stop()
  }

}
