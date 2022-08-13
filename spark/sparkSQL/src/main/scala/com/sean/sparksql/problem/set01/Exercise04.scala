package com.sean.sparksql.problem.set01

import org.apache.spark.sql.SparkSession

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 17:39
  * @Description 已知：table（名syc_mianshi），含有三个字段
  *              ，姓名Name(string)，消费时间DT（string），消费金额Money(Double)。
  *              记录条数有若干行。
  *              计算每个人在哪一天的消费金额最大
  */
object Exercise04 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local")
      .appName("Exercise04")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    spark.read
      .schema("name string,dt string,money double")
      .option("delimiter", "\t")
      .format("csv")
      .load("./sparkSQL/data/problem/set01/Exercise04/syc_mianshi.txt")
      .createOrReplaceTempView("syc_mianshi")

    spark.sql("select * from syc_mianshi").show()

    //row_number() over(partition by col1 order by col2 ) as rank
    //按照col1 分组，在每个组内按照col2排序从1打标签。
    //rank() over(partition by col1 order by col2 ):相同数据标号相同，不连续
    //dense_rank() over(partition by col1 order by col2 ) :相同数据标号相同，连续
    spark.sql(
      """
        |select
        |    t.name
        |    ,t.dt
        |from (
        |    select
        |        name
        |        ,dt
        |        ,row_number() over(partition by name order by money desc)   as  rn
        |    from syc_mianshi
        |) t
        |where t.rn = 1
      """.stripMargin).show()

    spark.stop()
  }

}
