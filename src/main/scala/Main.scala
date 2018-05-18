import _root_.CommunityDetection.{Louvain, LouvainConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import utils._


object Main {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Session configuration
    val conf = new SparkConf().set("spark.driver.maxResultSize", "8g")
    // Create the spark session first
    val ss = SparkSession.builder().config(conf).master("local").appName("tfidfApp").getOrCreate()

    // Spark context
    val sc = ss.sparkContext

    val DATASET_PATH = "./dataset/test.txt";

    //Import Dataset
    val importDataset = new ImportDataset()
    //Import edges file
    val dataset = importDataset.importTxt(sc, DATASET_PATH).map(item => item.split(" "))

    //Calculate common neighbors for each node
    val joinEdges = new JoinEdges()
    val commonNeighbors = joinEdges.getCommonNeighbors(ss, dataset)

    //Calculate weights for each edge
    val calculateWeights = new AssignWeigts()
    val allEdgesWithWeights = calculateWeights.ComputeWeight(commonNeighbors)

    allEdgesWithWeights.foreach(println)

    val edgesWithKWeights = calculateWeights.GetSubGraphWithKWeights(allEdgesWithWeights, 2)

    //TODO PLOT 2 GRAPHS

    val allEdgesWithoutWeights = calculateWeights.GetGraphWithDefaultWeights(allEdgesWithWeights)
    val subEdgesWithoutWeights = calculateWeights.GetGraphWithDefaultWeights(edgesWithKWeights)

    println("last step")
    allEdgesWithoutWeights.foreach(println)

    val config = LouvainConfig(50, 1)

    val louvain = new Louvain()
    louvain.run(sc, config, allEdgesWithoutWeights)
  }
}

