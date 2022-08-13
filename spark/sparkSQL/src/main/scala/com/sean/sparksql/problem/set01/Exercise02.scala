package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 17:12
  * @Description 使用SQL根据表A，表B 计算出表C
  */
object Exercise02 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Exercise02")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val aDF: DataFrame = spark.read
      .schema("date string,v1 string")
      .option("delimiter", "\t")
      .format("csv")
      .load("./sparkSQL/data/problem/set01/Exercise02/a.txt")
    aDF.show()
    aDF.createOrReplaceTempView("a")

    val bDF: DataFrame = spark.read
      .schema("date string,v2 string")
      .option("delimiter", "\t")
      .format("csv")
      .load("./sparkSQL/data/problem/set01/Exercise02/b.txt")
    bDF.show()
    bDF.createOrReplaceTempView("b")

    spark.sql(
      """
        |select
        |    nvl(a.`date`,b.`date`)  as  `date`
        |    ,nvl(a.v1,0)            as  v1
        |    ,nvl(b.v2,0)            as  v2
        |from a
        |full join b on a.`date` = b.`date`
      """.stripMargin).show()

    spark.stop()
  }

}
