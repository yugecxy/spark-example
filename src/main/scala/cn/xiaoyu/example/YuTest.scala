package cn.xiaoyu.example

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import cn.xiaoyu.example.myself.utils.EsUtils
import com.sksamuel.elastic4s.http.ElasticDsl.{createIndex, keywordField, mapping, textField}
import com.sksamuel.elastic4s.indexes.CreateIndexDefinition
import cn.xiaoyu.example.myself.utils.util

object YuTest {

  def esTest(spark: SparkSession, resDf: DataFrame) = {
    val indexName = "test"
    val create_index_dsl: CreateIndexDefinition =
      createIndex(indexName).mappings(
        mapping("default").fields(
          textField("x"),
          textField("y").index(false),
          keywordField("z")
        )
      )

    EsUtils.init_es_index(indexName, create_index_dsl)

    resDf.write.
      option("es.mapping.id", "x").
      mode(SaveMode.Append).
      format("es").
      save("test/default")
  }

  def main(args: Array[String]): Unit = {
    val spark = util.getSparkSession()
    import spark.implicits._
    val df = spark.sparkContext.makeRDD(Seq(
      ("a", "love job", "interest query","easy run"),
      ("b", "think life", "field default","project install")
    )).toDF("x", "y", "z","t")

    esTest(spark, df)

  }
}
