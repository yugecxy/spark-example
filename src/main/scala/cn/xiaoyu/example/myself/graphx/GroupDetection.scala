package cn.xiaoyu.example.myself.graphx

import org.apache.spark.graphx.lib.LabelPropagation
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel


object GroupDetection {
  /**
    * bd.group_detection
    */
  def sample(spark: SparkSession, relation: String): DataFrame = {
    //定义hive表的字段名
    val array = Array("user_id", "val", "addtime", "edge_id", "node_id", "sub_rel", "rel")

    //筛选掉脏数据
    val sourceSql =
      s"""
         |select
         |${array.mkString(",")}
         |from bd.group_detection
         |where val!='' and val is not null and val !='NULL'
         |and user_id is not null
         |and addtime is not null
         |and edge_id is not null
         |and node_id is not null
         |and rel='${relation}'
       """.stripMargin

    spark.sql(sourceSql)
  }

  /**
    * bd.user_classify
    */
  def getUserClassify(spark: SparkSession): DataFrame = {
    val classifyStr =
      s"""
         |select
         |user_id,
         |flag
         |from bd.user_classify
         |where flag is not null
       """.stripMargin

    spark.sql(classifyStr)
  }

  /**
    * 从样本数据踢出注册未发起借款的user
    *
    * @param source
    * @param userClassifyDF
    * @param spark
    * @return
    */
  def getFilterSource(source: DataFrame, userClassifyDF: DataFrame, spark: SparkSession) = {
    source.join(userClassifyDF.select("user_id"), "user_id")
  }

  /**
    * 生成节点
    *
    * @param source
    * @param spark
    */
  def genNodes(source: DataFrame): RDD[(VertexId, String)] = {

    //生成用户节点
    val userNodeRDD = source.select("user_id").rdd.
      distinct(1000).
      map(x => (x.get(0).toString.toLong, s"user_id::${x.get(0).toString}"))

    //生成属性节点
    val propertyNodeRDD = source.select("node_id", "rel", "val").rdd.
      distinct(1000).
      map(x => (x.get(0).toString.toLong, s"${x.getString(1)}::${x.getString(2)}"))

    userNodeRDD ++ propertyNodeRDD
  }

  /**
    * 生成边
    *
    * @param source
    * @param spark
    * @return
    */
  def genEdgs(source: DataFrame): RDD[Edge[(String)]] = {
    source.
      select("user_id", "node_id", "rel", "sub_rel").rdd.
      map(x => Edge(x.getLong(0), x.getLong(1), s"${x.getString(2)}:${x.getString(3)}"))
  }


  /**
    * 通过边和节点生成图
    *
    * @param filterSource
    * @return
    */
  def geneGraph(edgeRDD: RDD[Edge[(String)]], nodeRDD: RDD[(VertexId, String)]) = {
    Graph(nodeRDD, edgeRDD, null, edgeStorageLevel = StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY)

    //通过节点的邻居数去除脏数据（1到100）
    //注意不能把邻居数为0的筛掉（因为通过入度找到邻居数为0的是用户节点）否则边的数量为0，而节点数不为0
    //    originGraph.filter(x => {
    //      val filterVertex = x.collectNeighbors(EdgeDirection.In).filter(x => x._2.size <= 2 && x._2.size >= 1 || x._2.size > 100).map(x => (x._1, null))
    //      x.joinVertices(filterVertex)((vid, empty, user) => user)
    //    }, vpred = (a: VertexId, b: String) => b != null)
  }

  def toUserGraph(graph: Graph[String, String], spark: SparkSession) = {
    import spark.implicits._
    val propertyDF = graph.vertices.filter(_._2.split("::")(0) != "user_id").toDF("pid", "val_value")

    val oriUserEdgeRDD = graph.collectNeighborIds(EdgeDirection.In).
      filter(x => x._2.size > 2 && x._2.size <= 100).
      flatMap(row => {
        val commonList = row._2
        row._2.flatMap(x => for (l <- commonList if l != x) yield (row._1, Set(x, l)))
      }).distinct().map(x => (x._1, x._2.toSeq)).toDF("pid", "user_set").
      join(propertyDF, "pid").
      select("val_value", "user_set").rdd.map(x => (x.getSeq(1).toSet[Long], x.getString(0)))

    val userEdgeRDD = oriUserEdgeRDD.reduceByKey((a, b) => s"${a},${b}").
      map(x => (x._1.toList, x._2)).
      map(x => Edge(x._1(0), x._1(1), x._2))

    Graph.fromEdges(userEdgeRDD, 0, edgeStorageLevel = StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY)
    //判断是否有多边    originUserGraph.triplets.map(x => Set(x.toTuple._1._1, x.toTuple._2._1)).distinct()
  }

  def genResult(relation: String, table: String, spark: SparkSession) = {
    import spark.implicits._

    val sampleDF = sample(spark, relation)
    //获得用户信息
    val userClassifyDF = getUserClassify(spark)

    val source = getFilterSource(sampleDF, userClassifyDF, spark)

    userClassifyDF.persist(StorageLevel.MEMORY_ONLY)

    source.persist(StorageLevel.MEMORY_ONLY)

    //获取节点rdd
    val nodesRDD = genNodes(source)

    //获取边rdd
    val edgeRDD = genEdgs(source)

    //生成图
    val graph = geneGraph(edgeRDD, nodesRDD)

    val userGraph = toUserGraph(graph, spark)

    //    userGraph.collectNeighborIds(EdgeDirection.Either).map(x => x._2.size).sortBy(-_).take(100).foreach(println(_))

    //计算lpa
    val lpaGraph = LabelPropagation.run(userGraph, 5)

    //获取群组与属性值的对应关系
    val groupPropertyMap = lpaGraph.triplets.
      map(x => (Array(x.toTuple._1._2, x.toTuple._2._2), x.attr)).
      flatMap(x => for (k <- x._1) yield (k, x._2)).
      reduceByKey((a, b) => s"${a},${b}").
      map(x => (x._1, x._2, relation)).
      toDF("group_id", "values", "rel")

    //flag为0表示坏，1表示好
    val userCGDF = lpaGraph.vertices.toDF("user_id", "group_id").
      join(userClassifyDF.select("user_id", "flag"), "user_id").
      na.replace[Int]("flag", Map(1 -> 0, 2 -> 0, 3 -> 1))

    //注册udf以实现小数保留后两位小数
    def formatUdf: UserDefinedFunction = udf((x: Double) => x.formatted("%.2f").toDouble)

    val resultDF = userCGDF.groupBy("group_id").
      agg(sum("flag").as("good"), count("flag").as("total")).
      select(
        col("group_id"),
        formatUdf(col("good") / col("total")).as("good_rate"),
        formatUdf((col("total") - col("good")) / col("total")).as("bad_rate"),
        $"total"
      ).
      join(groupPropertyMap, "group_id")

    resultDF.write.mode(SaveMode.Append).partitionBy("rel").saveAsTable(table)

    //清理缓存
    userClassifyDF.unpersist()
    source.unpersist()
    graph.unpersist()
    userGraph.unpersist()
    lpaGraph.unpersist()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()


    //    val relation = "device_id"
    val relation = "phone"

    //测试群组大小
    //    lpaGraph.vertices.map(x => (x._2, 1)).reduceByKey((a, b) => a + b).sortBy(-_._2).take(100).foreach(println(_))
    genResult(relation, "bd.lpa_single", spark)


  }

}
