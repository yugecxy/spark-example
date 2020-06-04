package cn.xiaoyu.example

import org.apache.spark.{Partitioner, RangePartitioner}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.json4s._
import org.json4s.jackson.JsonMethods._

class MyPartitioner(partitions: Int) extends Partitioner {
  override def numPartitions: Int = partitions

  override def getPartition(key: Any): Int = 0
}


object JsonTest {
  lazy val spark = SparkSession.builder().master("local[6]")
    .config("spark.sql.files.maxPartitionBytes", 5 * 1024 * 1024)
    .config("parquet.block.size", 16 * 1024 * 1024)
    .getOrCreate()

  /**
    * S_VAL_PB_NEW,OPER_REV_TTM
    */
  def countDistribution(df: DataFrame, key: String) = {
    df.groupBy(key).agg(count(lit(1)).as("cnt")).orderBy(-col("cnt"))
  }

  def usePartitioner(df: DataFrame): DataFrame = {
    val schema = df.schema
    val patitioner = new MyPartitioner(200)
    val rdd = df.rdd.map(x => (x.getAs[Any]("idx_name"), x))
      .partitionBy(patitioner)
      .map(_._2)
    spark.createDataFrame(rdd, schema)
  }

  def main(args: Array[String]) {
    //    .filter(col("idx_name") === "S_VAL_MV")
    var df1 = spark.read.parquet("/Users/chenxiaoyu/Desktop/port=1_back")
      .filter(col("idx_name") === "S_VAL_PB_NEW" || col("idx_name") === "OPER_REV_TTM")
    //    df1 = usePartitioner(df1)
    //    countDistribution(df1, "idx_name").show()
    //    countDistribution(df1,"idx_name").show(1000)
    var df2 = spark.read.parquet("/Users/chenxiaoyu/Desktop/test_fast_join2")
    //    val patitioner = new MyPartitioner(200)
    //
    //    val rdd1 = df1.rdd.map(x => (x.getAs[Any]("idx_name"), x))
    //      .partitionBy(patitioner)
    //      .map(_._2)

    df1.join(broadcast(df2), df1("idx_name") === df2("t"), "right")
      //      .show()
      .write.mode(SaveMode.Overwrite).parquet("/Users/chenxiaoyu/Desktop/test_res")
    //    df.repartition(1).write.mode(SaveMode.Overwrite).parquet("/Users/chenxiaoyu/Desktop/test_fast")

    Thread.sleep(10000000)
  }
}

