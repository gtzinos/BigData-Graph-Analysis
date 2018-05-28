import _root_.CommunityDetection.{Louvain, LouvainConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.joda.time.{DateTime, DateTimeZone}
import utils._


object Main {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    // Session configuration
    val conf = new SparkConf().set("spark.driver.maxResultSize", "8g")
    // Create the spark session first
    val ss = SparkSession.builder().config(conf).master("local").appName("Community Detection").getOrCreate()

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

    val louvain = new Louvain()
    val louvainSub = new Louvain()

    //Save the time for all edges
    val allEdgesfromTimeMS = DateTime.now().getMillis()
    //Execute Louvain to all edges
    val alledgesAfterLouvain = louvain.run(sc, config, allEdgesWithoutWeights)
    println("All Edges (After Louvain) : " + alledgesAfterLouvain.count() + " Edges. Time to execute: " + (DateTime.now(DateTimeZone.UTC).getMillis() - allEdgesfromTimeMS) + " ms")

    //Save the time for sub graph
    val subGraphfromTimeMS = DateTime.now().getMillis()
    //Execute Louvain to sub graph
    val subEdgesAfterLouvain = louvainSub.run(sc, config, subEdgesWithoutWeights)
    println("Sub Graph (After Louvain) : " + subEdgesAfterLouvain.count() + " Edges. Time to execute: " + (DateTime.now(DateTimeZone.UTC).getMillis() - subGraphfromTimeMS) + " ms")


    /* Export data to csv */

    /* Export with weights */
    val exportData = new ExportData()
    //Export all edges with weights
    exportData.ExportEdgesToCsv(sc, allEdgesWithoutWeights, true, "./exports/allEdgesWithWeights.txt")
    //Export sub edges with k weights
    exportData.ExportEdgesToCsv(sc, subEdgesWithoutWeights, true, "./exports/edgesWithKWeights.txt")
    //Export all edges after louvain
    exportData.ExportEdgesToCsv(sc, alledgesAfterLouvain, true, "./exports/alledgesAfterLouvain.txt")
    //Export sub edges after louvain
    exportData.ExportEdgesToCsv(sc, subEdgesAfterLouvain, true, "./exports/subEdgesAfterLouvain.txt")
  }
}

