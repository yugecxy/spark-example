package cn.yuge.spark.streaming

import java.util.Date

import cn.yuge.spark.utils.StringsUtil
import com.alibaba.fastjson.JSON
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Created by chenxiaoyu
  * Author: chenxiaoyu
  * Date: 2018/5/31
  * Desc: 实时数据入hive
  *
  */
object BikeOrderKafkaToHive {
  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      System.err.print("### args error ###")
      System.exit(-1)
    }

    val batchTime = args(0).toInt

    val topics = Array("metrics-bike")

    val sparkConf = new SparkConf()
      .setAppName("BikeOrderKafkaToHive")
      .set("spark.streaming.kafka.maxRatePerPartition", "5000")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")
      .set("spark.streaming.kafka.consumer.poll.ms", "10000")
      .set("spark.streaming.ui.retainedBatches", "500")
      .set("spark.streaming.kafka.maxRetries", "3")

    val ssc = new StreamingContext(sparkConf, Seconds(batchTime))

    val sparkSession = new SparkSession.Builder().getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "10.111.20.155:9092,10.111.20.156:9092,10.111.20.157:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "bikeorder",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean), //取消偏移量自动提交
      "session.timeout.ms" -> "300000",
      "request.timeout.ms" -> "310000"
    )

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    //添加表字段schema
    val fields = Array("user_guid", "bike_no", "distance", "lat", "lng", "status", "create_time", "pt")
      .map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    stream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val resRdd: RDD[Row] = processRdd(rdd.map(_.value()))
      if (!resRdd.partitions.isEmpty) {
        val finalDf: DataFrame = sparkSession.createDataFrame(resRdd, schema)
        finalDf.repartition(1).write.mode(SaveMode.Append).partitionBy("pt").parquet("/user/hive/warehouse/bike.db/t_bike_appointment")
      }
      //线程安全
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })
    ssc.start()
    ssc.awaitTermination()
  }

  def processRdd(lineRdd: RDD[String]): RDD[Row] = {
    lineRdd.map(data => { //去除脏数据
      try {
        val time = StringsUtil.getRexString(data, "(,[\\d-]*?\\s[\\d:.]*?,)").replace(",", "")
        val pt = time.split(" ")(0).replace("-", "").trim
        if (pt.size != 8 || isPtValid(pt, 2, -2)) {
          null
        } else {
          val jsonData = StringsUtil.getRexString(data, "(\\{\"condition.+\\})") //提取json字符串
          JSON.parseObject(jsonData).getJSONObject("entityMata").getString("metric") //检测jsonData是否为合规的json
          (time, pt, jsonData)
        }
      } catch {
        case e: Exception => e.printStackTrace()
          null
      }
    })
      .filter(x => x != null)
      .map(data => {
        val metric = JSON.parseObject(data._3).getJSONObject("entityMata").getString("metric")
        metric match {
          case "Bike.Appointment" => (data._1, data._2, data._3, "0") //预约
          case "Bike.Outtime.Appointment" => (data._1, data._2, data._3, "1") //超时
          case "Bike.Cancel.Appointment" => (data._1, data._2, data._3, "2") //取消
          case "Bike.Auto.Cancel.Appointment" => (data._1, data._2, data._3, "3") //成功
          case _ => null
        }
      })
      .filter(x => x != null)
      .map(data => {
        val condition = JSON.parseObject(data._3).getJSONObject("condition")
        Row(condition.getString("userGuid"),
          condition.getString("bikeNo"),
          condition.getString("distance"),
          condition.getString("lat"),
          condition.getString("lng"),
          data._4, //status
          data._1, //time
          data._2 //pt
        )
      })
  }

  def isPtValid(ptStr: String, Day1: Int, Day2: Int) = {
    val date = FastDateFormat.getInstance("yyyyMMdd").parse(ptStr)
    val now = new Date
    date.after(org.apache.commons.lang3.time.DateUtils.addDays(now, Day1)) || date.before(org.apache.commons.lang3.time.DateUtils.addDays(now, Day2))
  }
}
