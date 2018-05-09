import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}

object Main {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Facebook ego net on graphx")

    /*
    val fbEdges: RDD[String] = sc.textFile("/Users/sherylj/GitHub/fb-graphx/facebook_combined.txt")
    val vertices1 = fbEdges.map(_.split(" ")).map(edge => edge(0).toInt).distinct
    val vertices2 = fbEdges.map(_.split(" ")).map(edge => edge(1).toInt).distinct
    val vertices: RDD[Int] = (vertices1 union vertices2).distinct
    val verticesRDD: RDD[(Long, Int)] = vertices.map( v => (v.toLong, v))
    val edgeArray: RDD[Edge[Int]] = fbEdges.map(_.split(" ")).map(edge => Edge(edge(0).toLong, edge(1).toLong))
    val graph = Graph(verticesRDD, edgeArray)
    Much better way is to use the Graph Loader to read the edgelist file
    */

    val graph = GraphLoader.edgeListFile(sc,"./dataset/facebook_combined.txt", numEdgePartitions = 4)

    graph.vertices.foreach(v => println(v))

    println("Number of vertices : " + graph.vertices.count())
    println("Number of edges : " + graph.edges.count())
    println("Triangle counts :" + graph.connectedComponents.triangleCount().vertices.collect().mkString("\n"));


  }
}