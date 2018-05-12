package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader, PartitionStrategy}

class AssignWeigts extends Serializable{

  // Calculate the number of triangles in which a node exists
  def ComputeWeight(sc:SparkContext, graph: Graph[Int,Int], pr:PartitionStrategy)={
    SelectPartitionStrategy(graph,PartitionStrategy.RandomVertexCut)
    // Find the triangle count for each vertex
    val triCounts = graph.triangleCount()

    triCounts
  }

  def SelectPartitionStrategy(graph: Graph[Int,Int],pr:PartitionStrategy): Unit ={
    graph.partitionBy(pr)
  }
}
