package cn.xiaoyu.example

import org.apache.spark.sql.{Column, Encoder, Row, SparkSession}
import org.apache.spark.sql.functions._
import java.io.FileWriter
import java.sql.Timestamp
import java.text.{DecimalFormat, SimpleDateFormat}
import java.util
import java.util.{Base64, Calendar, Date, GregorianCalendar}

import breeze.util.Encoder
import org.apache.commons.lang3.time.{DateUtils, FastDateFormat}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._

import scala.collection.{LinearSeq, SortedSet}
import scala.collection.immutable._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Random

object t {
  def apply(a: Int): Int = 9


}

case class m(a: String)


object test {

  lazy val spark = SparkSession.builder()
    .master("local[3]")
    .getOrCreate()

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val rdd1 = spark.sparkContext.makeRDD(Seq(
      ("a", 3),
      ("b", 4),
      ("b", 6)
    ))

    val rdd2 = spark.sparkContext.makeRDD(Seq(
      ("a", "yun"),
      ("b", "yu")
    ))

    val r1 = rdd1.reduceByKey((a, b) => a + b)
    val r2 = rdd2.reduceByKey((a, b) => a + b)
    r2.join(r1,7).foreach(println(_))
    Thread.sleep(1000000)
  }
}

