package cn.xiaoyu.example.myself.graphx

import java.security.MessageDigest
import java.util.Date
import org.apache.spark.graphx.{Edge, EdgeDirection, Graph, VertexId}
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object SinglePropertyGroupDetection {

  /**
    * bd.group_detection
    */
  def sample(spark: SparkSession): DataFrame = {
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
         |and rel not in ("ip","lbs")
         |and from_unixtime(addtime, 'yyyy-MM-dd') >='2017-01-01'
       """.stripMargin
    spark.sql(sourceSql)
  }

  /**
    * 通过边生成图
    *
    * @param filterSource
    * @return
    */
  def geneGraph(source: DataFrame) = {
    //边rdd生成
    val edgeRDD = source.
      select("user_id", "node_id").rdd.
      map(x => (x.getLong(0), x.getLong(1))).
      distinct(1500)

    //创建图
    val originGraph = Graph.fromEdgeTuples(edgeRDD, 0, edgeStorageLevel = StorageLevel.MEMORY_ONLY, vertexStorageLevel = StorageLevel.MEMORY_ONLY)

    //----------------属性关联人数查看--------------
    //    val vertexArray = originGraph.
    //      collectNeighbors(EdgeDirection.In).
    //      filter(x => x._2.length >= 15).
    //      map(x => (x._1, x._2.size)).
    //      sortBy(-_._2).take(800)
    //
    //    val vertexDF = spark.createDataFrame(vertexArray)
    //    vertexDF.toDF("node_id", "cnt").
    //      join(source.select("node_id", "rel", "val"), "node_id").
    //      distinct().
    //      orderBy("cnt").show(1000)
    //----------------------------------------------
    originGraph
  }

  /**
    * 全关系匹配计算
    */
  def allRelationMatchToHive(source: DataFrame, graph: Graph[Int, Int], spark: SparkSession): DataFrame = {
    import spark.implicits._
    val filterNodeIdDF = graph.
      collectNeighbors(EdgeDirection.In).
      filter(x => x._2.length >= 2 && x._2.length < 5000).
      map(x => x._1).toDF("node_id")

    val timeFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss")
    val createTime = FastDateFormat.getInstance("yyyy-MM-dd").format(new Date())

    filterNodeIdDF.
      join(source, "node_id").
      select("rel", "sub_rel", "val", "user_id", "addtime").rdd.
      map(row => {
        val addTime = if (row.get(4).toString.length == 10) row.getLong(4) * 1000 else row.getLong(4)
        (row.get(0).toString, row.get(1).toString, row.get(2).toString, row.get(3).toString, timeFormat.format(addTime), createTime)
      }).
      toDF("match_type", "match_subtype", "match_value", "match_user", "add_time", "create_time")
  }


  /**
    * 群关系匹配结果
    */
  def groupRelationMatchToHive(source: DataFrame, graph: Graph[Int, Int], spark: SparkSession): DataFrame = {
    import spark.implicits._
    val userClassifySql =
      s"""
         |select
         |user_id,last_order_nid,if_last_order_success,is_duotou,is_high_risk_reject,is_blacklist,is_overdue
         |from
         |bd.user_classify
       """.stripMargin
    val userClassifyDF = spark.sql(userClassifySql)
    val filterNodeIdGroupDF = graph.collectNeighbors(EdgeDirection.In).
      filter(x => x._2.length >= 20 && x._2.length < 5000).
      flatMap(x => {
        for (i <- x._2) yield (x._1, i._1, x._2.size)
      }).toDF("node_id", "user_id", "match_count")

    val filterSourceProperty = source.select("node_id", "val", "rel").distinct()

    val groupRelationInfo = filterNodeIdGroupDF.
      join(filterSourceProperty, "node_id").
      select("rel", "match_count", "val", "user_id").
      join(userClassifyDF, "user_id").
      select("rel", "match_count", "val", "last_order_nid", "if_last_order_success", "is_duotou", "is_high_risk_reject", "is_blacklist", "is_overdue", "user_id").rdd.
      map(x => ((x.get(0), x.get(1), x.get(2)), (x.get(3), x.get(4), x.get(5), x.get(6), x.get(7), x.get(8), x.get(9))))
    val createTime = FastDateFormat.getInstance("yyyy-MM-dd").format(new Date())
    groupRelationInfo.
      groupByKey().
      map(row => {
        var res = ""
        var userList = ""
        val match_count = row._1._2.toString.toFloat
        var lastOrderSuccess = 0f
        var lastOrderSuccessAndDue = 0f
        var lastOrderNid = 0f
        var lastOrderNidAndRisk = 0f
        var black = 0f
        var duotou = 0f
        row._2.map(x => {
          if (x._2 != null && x._2.toString.toInt == 1) lastOrderSuccess += 1
          if (x._2 != null && x._2.toString.toInt == 1 && x._6 != null && x._6.toString.toInt == 1) lastOrderSuccessAndDue += 1
          if (x._1 != null) lastOrderNid += 1
          if (x._1 != null && x._4 != null && x._4 == 1) lastOrderNidAndRisk += 1
          if (x._5 != null && x._5 == 1) black += 1
          if (x._3 != null && x._3 == 1) duotou += 1
          userList += x._7.toString + ","
        })
        if (lastOrderSuccess >= 10 && lastOrderSuccessAndDue / lastOrderSuccess >= 0.2) res += "1|"
        if (lastOrderNid < 10 && lastOrderNidAndRisk / lastOrderNid >= 0.5) res += "2|"
        if (black / match_count >= 0.3) res += "3|"
        if (duotou / match_count >= 0.8) res += "4|"
        val groupType = res.dropRight(1)
        val groupLevel = if (groupType == "") "0" else groupType
        val users = userList.dropRight(1)
        val badGroupHis = if (groupLevel == "0") null else s"{${groupLevel},${createTime}}"
        val groupId = MessageDigest.getInstance("MD5").digest((row._1._1.toString + row._1._3.toString).getBytes).map("%02x".format(_)).mkString
        (groupId, row._1._1.toString, row._1._3.toString, row._1._2.toString, users, groupLevel, badGroupHis, createTime, createTime)
      }).toDF("group_id", "match_type", "match_value", "match_count", "match_user", "group_level", "group_trace", "create_time", "update_time")
  }

  /**
    * 用户与群关系结果表
    */
  def userGroupToHive(source: DataFrame, groupRelation: DataFrame, graph: Graph[Int, Int], spark: SparkSession) = {
    import spark.implicits._
    val createTime = FastDateFormat.getInstance("yyyy-MM-dd").format(new Date())
    groupRelation.
      select("group_id", "match_user", "group_level").rdd.
      flatMap(x => {
        val levelCount = if (x.get(2) != "0") 1 else 0
        for (i <- x.getString(1).split(",")) yield (i, (x.get(0).toString, levelCount, 1))
      }).
      reduceByKey((a, b) => {
        (a._1 + "," + b._1, a._2 + b._2, a._3 + b._3)
      }).
      map(x => {
        (x._1, x._2._1, x._2._2, x._2._3, createTime)
      }).
      toDF("user_id", "match_group_id", "bad_group_cnt", "group_cnt", "create_time")
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().
      appName("MatchGroupDetection").
      config("spark.network.timeout", "300s").
      config("spark.executor.memory", "10g").
      config("spark.default.parallelism", 1500).
      config("spark.executor.instances", 30).
      config("spark.sql.shuffle.partitions", 1500).
      config("spark.executor.cores", 2).
      config("spark.shuffle.file.buffer", "128k").
      config("spark.reducer.maxSizeInFlight", "128m").
      getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    //读hive数据
    val source: DataFrame = sample(spark)
    source.persist(StorageLevel.MEMORY_ONLY)
    //    val nowTime = FastDateFormat.getInstance("_yyyy_MM_dd").format(new Date())
    //生成图
    val graph: Graph[Int, Int] = geneGraph(source)

    //-----------全匹配关系表计算--------------//
    val allRelationDF = allRelationMatchToHive(source, graph, spark)
    allRelationDF.write.partitionBy("create_time").mode(SaveMode.Append).saveAsTable("bigdata.all_relation")
    //-----------群关系匹配结果--------------//
    val groupRelationDF = groupRelationMatchToHive(source, graph, spark)
    groupRelationDF.cache()
    groupRelationDF.write.partitionBy("create_time").mode(SaveMode.Append).saveAsTable("bigdata.group_relation")

    //-----------用户与群关系结果表--------------//
    val userGroupDF = userGroupToHive(source, groupRelationDF, graph, spark)
    userGroupDF.write.partitionBy("create_time").mode(SaveMode.Append).saveAsTable("bigdata.user_group")
  }

}
