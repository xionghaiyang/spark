package com.sean.spark.entrance

import com.sean.spark.streaming.Kafka2HivePartition

object Entrance {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println(
        """
          |请输入两个参数
          |1.kafka2hivePartition
          |2.配置文件路径
        """.stripMargin)
      return
    }

    args(0) match {
      case "kafka2hivePartition" => Kafka2HivePartition.rsync(args(1))
      case _ => println("请输入正确的参数")
    }
  }

}
