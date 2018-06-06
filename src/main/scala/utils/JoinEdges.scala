package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class JoinEdges extends Serializable {

  //Return common neighbors for each edge
  def getCommonNeighbors(ss: SparkSession, dataset: RDD[Array[String]]) = {
    getNeighbors(ss, dataset)

    val merge: RDD[(Long, Long, Iterable[Long], Iterable[Long])] = ss.sql("" +
      " Select DISTINCT edg._1 as sourceId, edg._2 as targetId , nei._2 as sourceNeighbors, nei2._2 as targetNeighbors" +
      " from neighbors nei, allEdges edg, neighbors nei2" +
      " where nei._1 = edg._1 and nei2._1 = edg._2"
    )
      .rdd
      .map(row => (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long], row(2).asInstanceOf[Iterable[Long]], row(3).asInstanceOf[Iterable[Long]]))

    val commonNeighbors = merge
      .map(row => (row._1, row._2, row._3.filter(item => row._4.toList.contains(item))))

    commonNeighbors
  }

  //Neighbors for each node
  def getNeighbors(ss: SparkSession, dataset: RDD[Array[String]]) = {
    val edges = dataset.map(item => (item(0).toLong, item(1).toLong))
    val edgesGrouped = edges.groupByKey()

    val edgesReverted = dataset.map(item => (item(1).toLong, item(0).toLong))
    val edgesRevertedGrouped = edgesReverted.groupByKey()

    val neighbors = edgesGrouped.union(edgesRevertedGrouped).groupByKey().mapValues(item => item.flatten.toSeq)

    import ss.implicits._
    neighbors.toDF().createOrReplaceTempView("neighbors")

    edges.toDF().createOrReplaceTempView("allEdges")
  }
}
