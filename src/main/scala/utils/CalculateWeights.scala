package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Graph, GraphLoader, PartitionStrategy}
import org.apache.spark.rdd.RDD

class AssignWeigts extends Serializable{

  // Calculate the number of triangles in which pass through each edge
  def ComputeWeight(commonNeighbors: RDD[(Long, Long, Iterable[Long])])={

    val weights = commonNeighbors.
      map(row => (row._1, row._2, row._3, ((row._3.toList.length + 1) * 2) / 3))

    weights
  }

}
