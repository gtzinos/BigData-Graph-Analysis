import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.graphx._
import org.apache.spark.sql.SparkSession
import utils._


object Main {

  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //Session configuration
    val conf = new SparkConf().set("spark.driver.maxResultSize", "8g")
    // Create the spark session first
    val ss = SparkSession.builder().config(conf).master("local[8]").appName("tfidfApp").getOrCreate()
    //Import implicits
    //Spark context
    val sc = ss.sparkContext

    val DATASET_PATH="./dataset/test.txt";

    val importDataset = new ImportDataset()
    val filterDataset = new FilterEdges()
    val aw= new AssignWeigts()

    val graph = importDataset.ImportGraph(sc,DATASET_PATH)

    val allEdges = graph.edges.map(item => (item.srcId,item.dstId))
    val neighbors = graph.collectNeighbors(EdgeDirection.Either).groupByKey().mapValues(l3 => l3.flatMap(l4=>l4.map(l5 => l5._1):Seq[Long]).toSeq: Seq[Long])

    neighbors.foreach(println)

    allEdges.foreach(println)

    println()

    neighbors.join(allEdges).foreach(println)

    ss.createDataFrame(allEdges).createOrReplaceTempView("allEdges")

    ss.createDataFrame(neighbors).createOrReplaceTempView("neighbors")

    ss.sql("Select * from allEdges ae, neighbors ne " +
      "where ae._1 = ne._1 or ae._2 = ne._1" +
      "").show()
  }
}

      /*
      foreach(item => {
       
      print(item._1)
      print(item._2.foreach(tuplas => print(tuplas)))
      println()
    }

      )
*/

    //graph.vertices.foreach(v => println(v))

    //println("Number of vertices : " + graph.vertices.count())
    //println("Number of edges : " + graph.edges.count())
    // println("Number of total triangles : "+graph.connectedComponents().triangleCount())
    // println("Triangle counts :" + graph.connectedComponents.triangleCount().vertices.collect().mkString("\n"));

    //val weights=aw.ComputeWeight(sc,graph,PartitionStrategy.RandomVertexCut)


    //weights.vertices.collect().foreach(println)
/*
    weights.edges.map(f =>
    weights.vertices.map(
      cweights => cweights
    )).foreach(println)

    val mappedEdges = graph.edges.map(edge => (edge.srcId, edge.dstId))
    val mappedEdges2 = mappedEdges
*/
    //mappedEdges.foreach(println)

    //mappedEdges.join(mappedEdges2, _2).foreach(println)
    //mappedEdges.map(edge => (edge._1, mappedEdges.filter(edgef => edgef._1 == edge._2))).collect().foreach(println)

