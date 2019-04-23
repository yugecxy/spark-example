package cn.xiaoyu.example

import org.apache.spark.sql.{Column, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.io.FileWriter
import java.sql.Timestamp
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.{Calendar, GregorianCalendar}
import java.util.Date

import org.apache.commons.lang3.time.{DateUtils, FastDateFormat}

import scala.collection.mutable.ListBuffer
import scala.util.Random

object test {

  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.shuffle.sort.bypassMergeThreshold", "1")
    .getOrCreate()
//
  spark.sparkContext.setLogLevel("WARN")
  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val inputDF = spark.read.option("header", "true").csv("/Users/chenxiaoyu/Desktop/化工/化工source3/目录-表格 1.csv")
    println(inputDF.select("产品名称").as[String].collect().mkString("\",\""))
  }

}

