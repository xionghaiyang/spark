package com.sean.spark.streaming

import java.lang
import java.sql.{Connection, ResultSet}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import com.sean.spark.util._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object Kafka2HivePartitionOld {

  def rsync(profile: String): Unit = {
    println("kafka2hive任务开始")
    //获取环境以及配置信息
    val v_prop: Properties = ConfigurationManager.localProperties(profile)
    //批次间隔时间
    val batchDuration: Duration = Seconds(v_prop.getProperty("BATCHTIME").toInt)
    //topic名称
    val topicName: String = v_prop.getProperty("TOPICNAME")
    val topics: Set[String] = topicName.split(",").toSet
    //kafka集群服务地址及端口
    val brokers: String = v_prop.getProperty("BROKERS")
    //消费者组
    val groupId: String = v_prop.getProperty("GROUPID").toLowerCase
    //服务再启动位点（offset从什么地方开始）
    val reset: String = v_prop.getProperty("AUTO.OFFSET.RESET").toLowerCase
    //获取带分区的表名列表
    val ptTables: String = v_prop.getProperty("ptTable").toLowerCase
    val ptTableList: Array[String] = ptTables.split(",")
    //获取不带分区的表名列表
    val nonPtTables: String = v_prop.getProperty("nonPtTable").toLowerCase
    val nonPtTableList: Array[String] = nonPtTables.split(",")
    //需要从kafka中获取的表名列表
    val tabList: Array[String] = if (ptTables.length != 0 && nonPtTables.length == 0) {
      ptTableList
    } else if (ptTables.length == 0 && ptTables.length != 0) {
      nonPtTableList
    } else {
      ptTableList ++ nonPtTableList
    }
    //kafka相关认证
    val protocol: String = v_prop.getProperty("SECURITY.PROTOCOL")
    val serviceName: String = v_prop.getProperty("SASL.KERBEROS.SERVICE.NAME").toLowerCase
    val domainName: String = v_prop.getProperty("KERBEROS.DOMAIN.NAME").toLowerCase

    val spark: SparkSession = SparkSession.builder()
      .appName(topicName)
      .config("spark.streaming.stopGracefullyOnshutdown", "true")
      .config("spark.executor.heartbeatInterval", "200000")
      .config("spark.network.timeout", "300000")
      .config("spark.streaming.kafka.maxRatePerPartition", "1000")
      .config("spark.sql.shuffle.partitions", "10")
      //.config("spark.streaming.backpressure.enabled", "true")
      //.config("spark.streaming.backpressure.initialRate", "100")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext
    sc.setLogLevel("ERROR")
    println(s"applicationId=${sc.applicationId}")
    val ssc: StreamingContext = new StreamingContext(sc, batchDuration)

    val kafkaParams: collection.Map[String, Object] = collection.Map[String, Object](
      "bootstrap.servers" -> brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> groupId,
      "auto.offset.reset" -> reset,
      "enable.auto.commit" -> (false: lang.Boolean),
      "secutity.protocol" -> protocol,
      "sasl.kerberos.service.name" -> serviceName,
      "kerberos.domain.name" -> domainName
    )

    //存放索引的集合
    val offsets: mutable.Map[TopicPartition, Long] = mutable.Map[TopicPartition, Long]()

    //去mysql查询offset
    val connection: Connection = DataSourceUtil.getConnection
    val proxy: MysqlProxy = new MysqlProxy
    try {
      proxy.executeQuery(connection,
        "select * from offset_manager where groupid = ? and topic = ?",
        Array(groupId, topicName),
        new QueryCallback {
          override def process(rs: ResultSet): Unit = {
            while (rs.next()) {
              val topic: String = rs.getString(2)
              val partition: Int = rs.getInt(3)
              val offset: Long = rs.getLong(4)
              offsets += new TopicPartition(topic, partition) -> offset
            }
          }
        })
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      proxy.shutdown(connection)
    }

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = if (offsets.isEmpty) {
      println(s"offsets为空，从${reset}处开始消费")
      KafkaUtils.createDirectStream[String, String](ssc,
        PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams))
    } else {
      println("offsets不为空，从记录处开始消费")
      KafkaUtils.createDirectStream[String, String](ssc,
        PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](topics, kafkaParams, offsets))
    }

    var offsetRanges: Array[OffsetRange] = Array[OffsetRange]()

    inputDStream.transform((rdd: RDD[ConsumerRecord[String, String]]) => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    }).foreachRDD((rdd: RDD[ConsumerRecord[String, String]]) => {
      println("------------------新的批次开始---------------------")
      val batchCount: Long = rdd.count()
      println(s"新批次有${batchCount}条数据")
      if (batchCount > 0) {
        try {
          println(s"将业务数据存入hive，开始时间${Common.getNow()}")
          val name2JsonRDD: RDD[(String, String)] = rdd.mapPartitions((iter: Iterator[ConsumerRecord[String, String]]) => {
            val arrayBuffer: ArrayBuffer[(String, String)] = ArrayBuffer[(String, String)]()
            while (iter.hasNext) {
              val kafkaValue: String = iter.next().value()
              if (Common.isJson(kafkaValue) && kafkaValue.contains("tableName") && kafkaValue.contains("afterColumnList")) {
                val jSONObject: JSONObject = JSON.parseObject(kafkaValue)
                val tableName: String = jSONObject.getString("tableName")
                val beforeHandleJson: String = jSONObject.getString("afterColumnList")
                val afterHandleJson: String = beforeHandleJson.substring(1, beforeHandleJson.length - 1)
                arrayBuffer += ((tableName, afterHandleJson))
              }
            }
            arrayBuffer.iterator
          })
          name2JsonRDD.cache()

          tabList.foreach((tab: String) => {
            println(s"hive表为dwd_${tab}_part")
            val jsonRDD: RDD[Row] = name2JsonRDD.filter(kv => {
              tab.equalsIgnoreCase(kv._1) && Common.isJson(kv._2)
            }).values
              .mapPartitions((iter: Iterator[String]) => {
                val arrayBuffer: ArrayBuffer[Row] = ArrayBuffer[Row]()
                while (iter.hasNext) {
                  val json: String = iter.next()
                  val jSONObject: JSONObject = JSON.parseObject(json)
                  arrayBuffer += GetTableInfo.getRow(jSONObject, tab)
                }
                arrayBuffer.iterator
              })

            if (!jsonRDD.isEmpty()) {
              println(s"数据量有${jsonRDD.count()}")
              val structType: StructType = GetTableInfo.getSchema(tab)
              val dataFrame: DataFrame = spark.createDataFrame(jsonRDD, structType)
              val tmpTableName: String = s"dwd_${tab}_part_tmp"
              dataFrame.createOrReplaceTempView(tmpTableName)
              if (ptTableList.contains(tab)) {
                GetTableInfo.savePtTable(tab, tmpTableName, spark)
              } else if (nonPtTableList.contains(tab)) {
                GetTableInfo.saveNonPtTable(tab, tmpTableName, spark)
              }
            }
          })

          name2JsonRDD.unpersist()

          println(s"将业务数据存入hive，结束时间${Common.getNow()}")
        } catch {
          case e: Exception => e.printStackTrace()
            //保证offset正确行，当程序出现异常时直接退出程序，不让后续的offset写入到mysql,防止丢数据
            System.exit(0)
        }

        println(s"将偏移量存入mysql,开始时间${Common.getNow()}")
        val proxy: MysqlProxy = new MysqlProxy
        val connection: Connection = DataSourceUtil.getConnection
        try {
          for (or <- offsetRanges) {
            proxy.executeUpdate(connection,
              "replace into offset_manager(groupid,topic,`partition`,untilOffset) values(?,?,?,?)",
              Array(groupId,
                or.topic,
                or.partition,
                or.untilOffset)
            )
          }
        } catch {
          case e: Exception => e.printStackTrace()
        } finally {
          proxy.shutdown(connection)
        }
        println(s"将偏移量存入mysql,结束时间${Common.getNow()}")
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
