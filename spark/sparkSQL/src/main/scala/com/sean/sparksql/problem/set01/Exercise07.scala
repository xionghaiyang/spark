package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 19:55
  * @Description 公司某设备监控数据如下:(device_id:设备ID,state:设备状态，dt：监控时间)
  *              统计目标：统计出每个设备状态出现变化时前一条数据
  */
object Exercise07 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Exercise07")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val t3DF: DataFrame = spark.read
      .schema("device_id string,state int,dt string")
      .option("header", "true")
      .option("delimiter", ",")
      .csv("sparkSQL/data/problem/set01/Exercise07/MonitorData.txt")
    t3DF.show()
    t3DF.createOrReplaceTempView("t3")

    spark.sql(
      """
        |select
        |    t.device_id
        |    ,t.state
        |    ,t.dt
        |from (
        |    select
        |        device_id
        |        ,state
        |        ,dt
        |        ,size(collect_set(state) over(partition by device_id order by dt asc rows between current row and 1 following)) as  size
        |    from t3
        |) t
        |where t.size = 2
      """.stripMargin).show()

    spark.stop()
  }

}
