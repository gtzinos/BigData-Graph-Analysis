import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import utils._

object Main {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local", "Facebook ego net on graphx")

    val DATASET_PATH="./dataset/facebook_combined.txt";

    val importDataset = new ImportDataset()
    val aw= new AssignWeigts()

    val graph = importDataset.ImportGraph(sc,DATASET_PATH)

    graph.vertices.foreach(v => println(v))

    //println("Number of vertices : " + graph.vertices.count())
    //println("Number of edges : " + graph.edges.count())
    // println("Number of total triangles : "+graph.connectedComponents().triangleCount())
    // println("Triangle counts :" + graph.connectedComponents.triangleCount().vertices.collect().mkString("\n"));

    val weights=aw.ComputeWeight(sc,graph,PartitionStrategy.RandomVertexCut)

    weights.collect().foreach(println)
  }
}
