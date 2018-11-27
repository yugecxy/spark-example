import org.apache.spark.sql.SparkSession

object test {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val rdd = spark.sparkContext.makeRDD(Seq(("a", 82.0), ("b", 35.0), ("a", 94.0), ("a", 88.0)), 2)
    type MVType = (Double, Int) //定义一个元组类型(科目计数器,分数)
  }
}