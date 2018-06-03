package utils

import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.sql.SparkSession

class testing {

  def TriangleCount(graph: Graph[Int,Int], ss: SparkSession): Unit ={

    val allEdges = graph.edges.map(item => (item.srcId,item.dstId))
    val neighbors = graph.collectNeighbors(EdgeDirection.Either).groupByKey()
      .mapValues(l3 => l3.flatMap(l4=>l4.map(l5 => l5._1):Seq[Long]).toSeq: Seq[Long])

    neighbors.join(allEdges).foreach(println)

    ss.createDataFrame(allEdges).createOrReplaceTempView("allEdges")

    ss.createDataFrame(neighbors).createOrReplaceTempView("neighbors")

    val edgeDF = ss.sql("Select _1, _2 from allEdges")
      .withColumnRenamed("_1","ae1")
      .withColumnRenamed("_2","ae2").show()

    val neighborsDF = ss.sql("Select _1, _2 from neighbors")
      .withColumnRenamed("_1","n1")
      .withColumnRenamed("_2","n2").show()
  }

}
