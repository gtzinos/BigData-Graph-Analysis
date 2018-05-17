package utils

import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

class JoinEdges extends Serializable{

    def removeWeakEdges()  = {

    }

    def das2(graph: Graph[Int, Int], selectedIndex: Long) = {
       // val sourceNeighbors = graph.collectNeighbors(EdgeDirection.Either).zipWithIndex.filter({ case (neighbor, index) => selectedIndex == index } => neighbor._1.toInt == vertexSourceId)

       // val destNeighbors = graph.collectNeighbors(EdgeDirection.Either).filter(neightbor => neightbor._1 == vertexDestId)


        //sourceNeighbors.foreach(println)

       // graph.collectNeighbors(EdgeDirection.Either).foreach(ne => println(ne))

    }

    def calculateWeights(graph: Graph[Int, Int]) : Unit = {
        graph.edges.foreach(edge => {
            //  getCommonNeighbors(graph, edge.srcId.toInt, edge.dstId.toInt)
        })
    }

    def getNeighbors(ss: SparkSession, dataset: RDD[Array[String]], allEdges: RDD[(Long, Long)]) = {
        val edges  = dataset.map(item => (item(0).toLong, item(1).toLong))
        val edgesReverted = dataset.map(item => (item(1).toLong, item(0).toLong))

        val edgesGrouped = edges.groupByKey()
        val edgesRevertedGrouped = edgesReverted.groupByKey()

        val neighbors = edgesGrouped.union(edgesRevertedGrouped).groupByKey().mapValues(item => item.flatten.toSeq)

        neighbors.foreach(println)

        import ss.implicits._
        neighbors.toDF().createOrReplaceTempView("neighbors")

        allEdges.toDF().createOrReplaceTempView("allEdges")
        allEdges.foreach(println)
    }

    def getCommonNeighbors(ss: SparkSession, dataset: RDD[Array[String]], allEdges: RDD[(Long, Long)]) = {
        getNeighbors(ss, dataset, allEdges)

        val merge: RDD[(Long, Long, Iterable[Long], Iterable[Long])] = ss.sql("" +
          " Select DISTINCT edg._1 as sourceId, edg._2 as targetId , nei._2 as sourceNeighbors, nei2._2 as targetNeighbors" +
          " from neighbors nei, allEdges edg, neighbors nei2"+
          " where nei._1 = edg._1 and nei2._1 = edg._2"
        )
          .rdd
          .map(row => (row(0).asInstanceOf[Long], row(1).asInstanceOf[Long], row(2).asInstanceOf[Iterable[Long]], row(3).asInstanceOf[Iterable[Long]]))

        val commonNeighbors = merge
          .map(row => (row._1, row._2, row._3.filter(item => row._4.toList.contains(item))))

        commonNeighbors
    }
}
