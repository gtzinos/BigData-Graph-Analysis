package utils

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD

class AssignWeigts extends Serializable {

  // Calculate the number of triangles in which pass through each edge
  def ComputeWeight(commonNeighbors: RDD[(Long, Long, Iterable[Long])]) = {
    // ((Nodes length + 2 - 1 ) * 2 ) / 3
    val weights = commonNeighbors.
      map(row => (row._1, row._2, row._3, ((row._3.toList.length + 1) * 2) / 3))

    weights
  }

  // Returns a new subgraph, filtered by weights
  def GetSubGraphWithKWeights(edgesWithWeights: RDD[(Long, Long, Iterable[Long], Int)], kWeights: Long) = {
    edgesWithWeights.filter(edge => edge._4 >= kWeights)
  }

  // Returns a new graph without neighbors
  def GetGraphWithDefaultWeights(edgesWithWeights: RDD[(Long, Long, Iterable[Long], Int)]) = {
    val withoutWeights: RDD[Edge[Long]] = edgesWithWeights.map(edge => Edge(edge._1: Long, edge._2: Long, 1L))

    withoutWeights
  }

}
