package com.sean.spark.util

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Common {

  def getTime(v_pattern: String): String = {
    val sdf: SimpleDateFormat = new SimpleDateFormat(v_pattern)
    sdf.format(new Date)
  }

  def getNow(): String = {
    getTime("yyyy-MM-dd HH:mm:ss")
  }

  def getToday(): String = {
    getTime("yyyy-MM-dd")
  }

  def getHour(): String = {
    getTime("HH")
  }

  def addDay(v_date: String, v_addDay: Int, v_sourcePattern: String, v_sinkPattern: String): String = {
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(new SimpleDateFormat(v_sourcePattern).parse(v_date))
    calendar.add(Calendar.DATE, v_addDay)
    new SimpleDateFormat(v_sinkPattern).format(calendar.getTime)
  }

  val getAddDay: (Int, String) => String = addDay(getNow(), _, "yyyy-MM-dd HH:mm:ss", _)

  def addHour(v_date: String, v_addHour: Int, v_sourcePattern: String, v_sinkPattern: String): String = {
    val calendar: Calendar = Calendar.getInstance()
    calendar.setTime(new SimpleDateFormat(v_sourcePattern).parse(v_date))
    calendar.add(Calendar.HOUR, v_addHour)
    new SimpleDateFormat(v_sinkPattern).format(calendar.getTime)
  }

  val getAddHour: (Int, String) => String = addDay(getNow(), _, "yyyy-MM-dd HH:mm:ss", _)

  def isJson(content: String): Boolean = {
    try {
      val jSONObject: JSONObject = JSON.parseObject(content)
      true
    } catch {
      case e: Exception => e.printStackTrace()
        false
    }
  }

  def mergeFilePartition(database: String, tableName: String, fileNum: Int, partColumn: String, part: String, spark: SparkSession): Unit = {
    spark.sql(s"use ${database}")
    val df: DataFrame = spark.sql(s"select * from ${tableName} where ${partColumn} = '${part}'")
    val columns: Array[String] = df.columns
    val afterColumns: Array[String] = columns.dropRight(1)

    df.coalesce(fileNum)
      .write
      .mode(SaveMode.Overwrite)
      .saveAsTable(s"${tableName}_mergeFile")

    //println("删除原分区")
    spark.sql(s"alter table ${tableName} drop partition(${partColumn} = ${part})")

    try {
      println("插入合并小文件之后的数据")
      spark.sql(
        s"""
           |insert into table ${tableName} partition (${partColumn} = '${part}')
           |select ${afterColumns.mkString(",")} from ${tableName}_mergeFile
        """.stripMargin)
    } catch {
      case e: Exception => e.printStackTrace()
        //System.exit(0)
        return
    }

    println("清空临时表")
    spark.sql(s"truncate table ${tableName}_mergeFile")

    println(s"${tableName}表${partColumn}=${part}分区的小文件合并完成，合并后文件数为:${fileNum}")
  }

}
