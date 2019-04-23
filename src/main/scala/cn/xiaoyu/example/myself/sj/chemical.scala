package cn.xiaoyu.example.myself.sj

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object chemical {
  val spark = SparkSession.builder()
    .master("local[1]")
    .config("spark.shuffle.sort.bypassMergeThreshold", "1")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  import spark.implicits._

  def main(args: Array[String]): Unit = {
    val inputDF = spark.read.option("header", "true").csv("/Users/chenxiaoyu/Desktop/化工/化工source3")
    val csvPaths = inputDF
      .select(regexp_replace(input_file_name(), "%20", " ").as("input_file_name"))
      .distinct()
      .as[String]
      .collect()
      .filter(x => !x.contains("开工率") && !x.contains("目录"))

    csvPaths.foreach(path => {
      val df = spark.sparkContext.textFile(path)
      var res = df.collect().map(_ + "\n").mkString("").replaceAll("(\"[^\"]*?)(\n)([^\"]*?\")", "$1$3")
      spark.sparkContext.makeRDD(res.split("\n").toSeq).repartition(1)
        .saveAsTextFile(path.replaceAll("-表格 1.csv", "_name").replaceAll("化工source", "化工"))
    })
  }
}
