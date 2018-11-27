package cn.yuge.spark.streaming

import java.lang.Long
import java.sql.{Date}
import java.text.SimpleDateFormat

import com.alibaba.fastjson.JSON
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import com.fasterxml.jackson.databind.deser.std.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, StreamingContext}
import scala.collection.JavaConverters._

/**
  * hellobike需求，每5分钟聚合一次，统计5分钟内的app平均启动时长和5分钟内的app启动次数
  */
object AppLoadDelay {


  var pointTime: Long = null

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      System.err.print("### args error ###")
      System.exit(-1)
    }
    val conf = new SparkConf().setAppName("AppLoadDelay")
      .set("spark.streaming.kafka.maxRatePerPartition", "5000")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.kafka.consumer.poll.ms", "10000")
      .set("spark.streaming.ui.retainedBatches", "500")
      .set("spark.streaming.kafka.maxRetries", "3")

    val ssc = new StreamingContext(conf, Durations.seconds(args(0).toInt))
    val groupId = args(1).toString
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.111.20.3:9092,10.111.20.4:9092,10.111.20.5:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "security.protocol" -> "SASL_PLAINTEXT",
      "sasl.mechanism" -> "PLAIN",
      "auto.offset.reset" -> "latest",
      "group.id" -> groupId,
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topics = Array("bi-ubt")
    try {
      val stream = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )
      var offsetRanges = Array[OffsetRange]()
      stream.foreachRDD { rdd =>
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        pointTime = System.currentTimeMillis()
        if (!rdd.isEmpty()) {
          processRDD(rdd.map(_.value()))
        } else {
          writeToDb("unknow", Long.valueOf(0), 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
        }
      }
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    } catch {
      case e: Exception => "error_info:" + e.printStackTrace()
    }
    ssc.start
    ssc.awaitTermination
  }


  def processRDD(lineRdd: RDD[String]): Unit = {

    lineRdd.map(data => {
      try {
        val json = data.substring(data.indexOf(",{") + 1, data.length)
        val progressKey = JSON.parseObject(json).getJSONObject("condition").getString("progressKey")
        (progressKey, data)
      } catch {
        case e: Exception => e.printStackTrace()
          ("ERROR", data)
      }
    })
      .filter(_._1 != "ERROR")
      .groupByKey()
      .map(x => {
        var minTime: Long = Long.MAX_VALUE
        var maxTime, configDataTime, rideConfigTime = 0L
        val format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
        var appVersion, netWork, osType = ""
        x._2.foreach(line => {
          try {
            val time = line.substring(line.indexOf(",", 1) + 1, line.indexOf(",", line.indexOf(",", 1) + 1))
            val logTime: Long = format.parse(time).getTime
            if (logTime > maxTime) {
              maxTime = logTime
            }
            else {
              minTime = logTime
            }
            val json = line.substring(line.indexOf(",{") + 1, line.length)
            val jsonData = JSON.parseObject(json).getJSONObject("condition")
            val actionName = jsonData.getString("actionName")
            val status = jsonData.getString("status")
            appVersion = jsonData.getString("appversion")
            netWork = jsonData.getString("network")
            osType = jsonData.getString("ostype")
            if (actionName == "client.init.getConfigData" && status == "2") {
              configDataTime = logTime
            }
            if (actionName == "client.init.getRideConfig" && status == "1") {
              rideConfigTime = logTime
            }
          } catch {
            case e: Exception => e.printStackTrace()
          }
        })
        val adverTime = Math.abs(configDataTime - rideConfigTime)
        val duraction = maxTime - minTime - adverTime
        (appVersion, netWork.toLowerCase, osType.toLowerCase, duraction.toDouble)
      })
      .filter(_._4 > 0)
      .filter(_._4 < 60000)
      .filter(x => {
        if (x._1 != null & x._1 != "" & x._2 != null & x._2 != "" & x._3 != null & x._3 != "") true else false
      })
      .map(data => {
        val appVersion = data._1
        val netWork =
          data._2 match {
            case "2g" => "2G"
            case "3g" | "4g" => "3G/4G"
            case "wifi" => "Wifi"
            case _ => "其他"
          }
        val osType = data._3
        val duraction = data._4
        var du1sLow, du1To2s, du2To3s, du3To4s, du4To5s, du5To6s, du6To7s, du7To8s, du8To9s, du9To10s, du10Over = 0
        duraction match {
          case `duraction` if duraction < 1000 => du1sLow = 1
          case `duraction` if (duraction >= 1000 & duraction < 2000) => du1To2s = 1
          case `duraction` if (duraction >= 2000 & duraction < 3000) => du2To3s = 1
          case `duraction` if (duraction >= 3000 & duraction < 4000) => du3To4s = 1
          case `duraction` if (duraction >= 4000 & duraction < 5000) => du4To5s = 1
          case `duraction` if (duraction >= 5000 & duraction < 6000) => du5To6s = 1
          case `duraction` if (duraction >= 6000 & duraction < 7000) => du6To7s = 1
          case `duraction` if (duraction >= 7000 & duraction < 8000) => du7To8s = 1
          case `duraction` if (duraction >= 8000 & duraction < 9000) => du8To9s = 1
          case `duraction` if (duraction >= 9000 & duraction < 10000) => du9To10s = 1
          case `duraction` if (duraction >= 10000) => du10Over = 1
        }
        (s"${appVersion}##${netWork}##${osType}", (duraction, 1, du1sLow, du1To2s, du2To3s, du3To4s, du4To5s, du5To6s, du6To7s, du7To8s, du8To9s, du9To10s, du10Over))
      }
      ).filter(_._1 != "")
      .reduceByKey(
        (a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4, a._5 + b._5, a._6 + b._6, a._7 + b._7, a._8 + b._8, a._9 + b._9, a._10 + b._10, a._11 + b._11, a._12 + b._12, a._13 + b._13)
      )
      .map(data => {
        (data._1, data._2._1 / data._2._2, (data._2._3, data._2._4, data._2._5, data._2._6, data._2._7, data._2._8, data._2._9, data._2._10, data._2._11, data._2._12, data._2._13))
      }
      ).foreach(data => {
      writeToDb(data._1, data._2.toLong, data._3._1, data._3._2, data._3._3, data._3._4, data._3._5, data._3._6, data._3._7, data._3._8, data._3._9, data._3._10, data._3._11)
    })
  }

  def writeToDb(key: String, avgTime: Long, du1sLow: Int, du1To2s: Int, du2To3s: Int, du3To4s: Int, du4To5s: Int, du5To6s: Int, du6To7s: Int, du7To8s: Int, du8To9s: Int, du9To10s: Int, du10Over: Int): Unit = {
    val appVersion = key.split("##")(0)
    val netWork = key.split("##")(1)
    val osType = key.split("##")(2)
    val ptime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(pointTime))
    val contentMap = Map("averageTimeByMinute" -> avgTime, "pointTime" -> ptime, "du1sLow" -> du1sLow, "du1To2s" -> du1To2s, "du2To3s" -> du2To3s, "du3To4s" -> du3To4s, "du4To5s" -> du4To5s, "du5To6s" -> du5To6s, "du6To7s" -> du6To7s, "du7To8s" -> du7To8s, "du8To9s" -> du8To9s, "du9To10s" -> du9To10s, "du10Over" -> du10Over)
    val content = JSON.toJSON(contentMap.asJava).toString
    //    OssBikeDao.addOrUpdateAppLoadDelay(appVersion, netWork, osType, new Timestamp(pointTime), content)
  }
}
