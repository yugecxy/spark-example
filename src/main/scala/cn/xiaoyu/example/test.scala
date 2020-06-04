package cn.xiaoyu.example

import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = 0
}


object JsonTest {
  lazy val spark = SparkSession.builder().master("local[4]")
    .config("spark.sql.files.maxPartitionBytes", 5 * 1024 * 1024)
    .config("parquet.block.size", 16 * 1024 * 1024)
    .getOrCreate()

  def save() = {
    val conUrl = s"jdbc:mysql://rm-6wep82uuvdggcd4k1po.mysql.japan.rds.aliyuncs.com:3306/test?characterEncoding=utf-8&zeroDateTimeBehavior=CONVERT_TO_NULL&useSSL=false&connectTimeout=10000&serverTimezone=UTC"
    val df = spark.read.parquet("/Users/chenxiaoyu/Desktop/test.snappy.parquet")
    println(df.count())
    println("begin to save...")

    df.write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .options(Map(
        "url" -> conUrl,
        "dbtable" -> "1w_one_part_test",
        "driver" -> "com.mysql.cj.jdbc.Driver",
        "user" -> "chenxiaoyu",
        "password" -> "chenxiaoyu123"
      )).save()
    println("end to save...")
  }


  def main(args: Array[String]) {
    var df1 = spark.read.parquet("/Users/chenxiaoyu/Desktop/test_fast")
    var df2 = spark.read.parquet("/Users/chenxiaoyu/Desktop/test_fast_join2")
    val patitioner = new MyPartitioner(200)

    val rdd1 = df1.rdd.map(x => (x.getAs[Any]("idx_name"), x))
      .partitionBy(patitioner)
      .map(_._2)

    df1.join(broadcast(df2), df1("idx_name") === df2("t"), "right")
      .write.mode(SaveMode.Overwrite).parquet("/Users/chenxiaoyu/Desktop/test_res")
    //    Thread.sleep(1000000)

    //    df.repartition(1).write.mode(SaveMode.Overwrite).parquet("/Users/chenxiaoyu/Desktop/test_fast")
  }
}

