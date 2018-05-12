package utils

import org.apache.spark.graphx.{EdgeDirection, Graph}

class FilterEdges extends Serializable{

    def removeWeakEdges()  = {

    }

    def getCommonNeighbors(graph: Graph[Int, Int], selectedIndex: Long) = {
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


}
