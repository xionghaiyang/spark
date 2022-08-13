package com.sean.sparksql.problem.set01

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * @Author xionghaiyang
  * @Date 2022-08-13 20:03
  * @Description 有如下数据，用户注册信息表和用户登录信息表：
  *              SparkSQL统计注册日后1日、2日、3日、4日、5日、6日、7日用户留存数
  */
object Exercise08 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName("Exercise08")
      .master("local")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val regist_infosDF: DataFrame = spark.read
      .schema("uid string,regist_dt string,regist_os string")
      .option("header", "true")
      .option("delimiter", ",")
      .csv("sparkSQL/data/problem/set01/Exercise08/regist_infos.csv")
    regist_infosDF.show()
    regist_infosDF.createOrReplaceTempView("regist_infos")

    val login_infosDF: DataFrame = spark.read
      .schema("uid string,login_dt string")
      .option("header", "true")
      .option("delimiter", ",")
      .csv("sparkSQL/data/problem/set01/Exercise08/login_infos.csv")
    login_infosDF.show(1000, true)
    login_infosDF.createOrReplaceTempView("login_infos")

    spark.sql(
      """
        |select
        |    t.regist_dt
        |    ,t.days
        |    ,count(t.uid)   as  cnt
        |from (
        |    select
        |        t1.uid
        |        ,t1.regist_dt
        |        ,datediff(from_unixtime(unix_timestamp(t2.login_dt,'yyyyMMdd'),'yyyy-MM-dd'),from_unixtime(unix_timestamp(t1.regist_dt,'yyyyMMdd'),'yyyy-MM-dd'))   as  days
        |    from regist_infos t1
        |    inner join login_infos t2 on t1.uid = t2.uid
        |) t
        |where t.days between 1 and 7
        |group by
        |    t.regist_dt
        |    ,t.days
      """.stripMargin).show()

    spark.stop()
  }

}
