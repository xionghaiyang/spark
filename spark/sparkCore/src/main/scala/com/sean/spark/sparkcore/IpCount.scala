package com.sean.spark.sparkcore

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import scala.util.control.Breaks._

object IpCount {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf()
      .setMaster("local[4]")
      .setAppName("IpCount")
    val sc: SparkContext = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val ipCollect: Array[(Long, Long, String, String)] = sc.textFile("file://" + ClassLoader.getSystemResource("ip.txt").getPath)
      .map(_.split("\\|"))
      .map(x => (x(0), x(1), x(x.length - 2), x(x.length - 1)))
      .map(x => (ipToLong(x._1), ipToLong(x._2), x._3, x._4))
      .collect()
    val broadcast: Broadcast[Array[(Long, Long, String, String)]] = sc.broadcast(ipCollect)

    val ipAndOneRDD: RDD[((String, String), Int)] = sc.textFile("file://" + ClassLoader.getSystemResource("user.txt").getPath)
      .map(_.split("\\|")(1))
      .mapPartitions(iter => {
        val ipArray: Array[(Long, Long, String, String)] = broadcast.value
        iter.map(ip => {
          val ipIndex: Int = ipToIndex(ip, ipArray)
          val ipTuple: (Long, Long, String, String) = ipArray(ipIndex)
          ((ipTuple._3, ipTuple._4), 1)
        })
      })
    val ipCountRDD: RDD[((String, String), Int)] = ipAndOneRDD.reduceByKey(_ + _)
    ipCountRDD.foreach(println)

    sc.stop()
  }

  def ipToIndex(ip: String, ipArray: Array[(Long, Long, String, String)]): Int = {
    val ipLong: Long = ipToLong(ip)
    var startIndex = 0
    var endIndex = ipArray.length - 1
    var middleIndex = (startIndex + endIndex) / 2
    breakable {
      while (startIndex <= endIndex) {
        if (ipLong >= ipArray(startIndex)._1 && ipLong >= ipArray(endIndex)._2) {
          break
        }
        if (ipLong <= ipArray(startIndex)._1) {
          endIndex = middleIndex - 1
        }
        if (ipLong >= ipArray(endIndex)._2) {
          startIndex = middleIndex + 1
        }
        middleIndex = (startIndex + endIndex) / 2
      }
    }
    middleIndex
  }

  def ipToLong(ip: String): Long = {
    val split: Array[String] = ip.split("\\.")
    var ipNum = 0L
    for (i <- 0 until split.length) {
      ipNum = split(0).toLong | ipNum << 8L
    }
    ipNum
  }

}
