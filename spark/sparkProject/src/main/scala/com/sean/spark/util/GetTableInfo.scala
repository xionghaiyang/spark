package com.sean.spark.util

import com.alibaba.fastjson.JSONObject
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object GetTableInfo {

  def savePtTable(tab: String, tmpTableName: String, spark: SparkSession): Unit = {
    spark.sql("use lsw_base")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    //yyyyMMdd
    val pt: String = Common.getToday().replace("-", "")
    //println(s"pt=${pt}")
    tab match {
      case "table1" => {
        spark.sql(
          s"""
             |select
             | c1
             | ,c2
             | ,c3
             | ,'${pt}' as pt
             |from ${tmpTableName}
          """.stripMargin)
          .coalesce(1)
          .write
          .format("Hive")
          .partitionBy("pt")
          .mode("append")
          .saveAsTable("dwd_table1_part")
      }
      case "table2" => {
        spark.sql(
          s"""
             |select
             | c1
             | ,c2
             | ,c3
             | ,'${pt}' as pt
             |from ${tmpTableName}
          """.stripMargin)
          .coalesce(1)
          .write
          .format("Hive")
          .partitionBy("pt")
          .mode("append")
          .saveAsTable("dwd_table2_part")
      }
      case _ =>
    }
  }

  def saveNonPtTable(tab: String, tmpTableName: String, spark: SparkSession): Unit = {
    //TODO
  }

  def getSchema(tab: String): StructType = {
    var structType: StructType = null
    tab match {
      case "table1" =>
        structType = StructType(List(
          StructField("c1", StringType, nullable = true),
          StructField("c2", StringType, nullable = true),
          StructField("c3", StringType, nullable = true)
        ))
      case "table2" =>
        structType = StructType(List(
          StructField("c1", StringType, nullable = true),
          StructField("c2", StringType, nullable = true),
          StructField("c3", StringType, nullable = true)
        ))
      case _ =>
    }
    structType
  }

  def getRow(tabJson: JSONObject, tab: String): Row = {
    var row: Row = null
    tab match {
      case "table1" =>
        row = Row(tabJson.getString("C1"),
          tabJson.getString("C2"),
          tabJson.getString("C3")
        )
      case "table2" =>
        row = Row(tabJson.getString("C1"),
          tabJson.getString("C2"),
          tabJson.getString("C3")
        )
      case _ =>
    }
    row
  }

}