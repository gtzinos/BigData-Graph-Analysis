import _root_.CommunityDetection.{Louvain, LouvainConfig}
import co.theasi.plotly.Plot
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

    //Calculate weights for each edge based on triangles pass through each edge
    val calculateWeights = new AssignWeigts()
    val allEdgesWithWeights = calculateWeights.ComputeWeight(commonNeighbors)

    //Filter edges by weight
    val edgesWithKWeights = calculateWeights.GetSubGraphWithKWeights(allEdgesWithWeights, 2)

    //Remove calculated weights
    val allEdgesWithoutWeights = calculateWeights.GetGraphWithDefaultWeights(allEdgesWithWeights)
    val subEdgesWithoutWeights = calculateWeights.GetGraphWithDefaultWeights(edgesWithKWeights)

    /*
      Community Detection with Louvain
    */

    //Create config
    val config = LouvainConfig(1, 1)

    //Execute Louvain to all edges
    val louvain = new Louvain()
    val alledgesAfterLouvain = louvain.run(sc, config, allEdgesWithoutWeights)

    //Execute Louvain to sub graph
    val louvainSub = new Louvain()
    val subEdgesAfterLouvain = louvainSub.run(sc, config, subEdgesWithoutWeights)

    /* Export data to csv */

    /* Export with weights */
    val exportData = new ExportData()
    //Export all edges with weights
    exportData.ExportEdgesToCsv(allEdgesWithoutWeights, true, "./exports/allEdgesWithWeights.txt")
    //Export sub edges with k weights
    exportData.ExportEdgesToCsv(subEdgesWithoutWeights, true, "./exports/edgesWithKWeights.txt")
    //Export all edges after louvain
    exportData.ExportEdgesToCsv(alledgesAfterLouvain, true, "./exports/alledgesAfterLouvain.txt")
    //Export sub edges after louvain
    exportData.ExportEdgesToCsv(subEdgesAfterLouvain, true, "./exports/subEdgesAfterLouvain.txt")
  }
}

