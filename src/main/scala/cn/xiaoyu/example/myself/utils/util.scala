package cn.xiaoyu.example.myself.utils

import org.apache.spark.sql.SparkSession
import org.apache.commons.configuration.PropertiesConfiguration

object util {
  val prop: PropertiesConfiguration = new PropertiesConfiguration("config.properties")

  def getSparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[2]")
      .config("es.nodes", prop.getString("es.nodes"))
      .config("es.nodes.wan.only", prop.getString("es.nodes.wan.only"))
      .config("es.mapping.date.rich","false")
      .getOrCreate()
  }

}
