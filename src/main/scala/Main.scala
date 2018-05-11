import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import utils._

object Main {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)
  
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Facebook ego net on graphx")

    val DATASET_PATH="./dataset/test.txt";

    val importDataset = new ImportDataset()
    val aw= new AssignWeigts()

    val graph = importDataset.ImportGraph(sc,DATASET_PATH)

    //graph.vertices.foreach(v => println(v))

    //println("Number of vertices : " + graph.vertices.count())
    //println("Number of edges : " + graph.edges.count())
    // println("Number of total triangles : "+graph.connectedComponents().triangleCount())
    // println("Triangle counts :" + graph.connectedComponents.triangleCount().vertices.collect().mkString("\n"));

    val weights=aw.ComputeWeight(sc,graph,PartitionStrategy.RandomVertexCut)

    weights.vertices.collect().foreach(println)

    val mappedEdges = graph.edges.map(edge => (edge.srcId, edge.dstId))
    val mappedEdges2 = mappedEdges

    //mappedEdges.foreach(println)

    //mappedEdges.join(mappedEdges2, _2).foreach(println)
    //mappedEdges.map(edge => (edge._1, mappedEdges.filter(edgef => edgef._1 == edge._2))).collect().foreach(println)

  }
}
