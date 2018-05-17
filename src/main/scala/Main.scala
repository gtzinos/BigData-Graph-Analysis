import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import utils._

import scala.collection.mutable


 abstract class merger(var nei1: Long, var nei2: Array[Long], var edg1: Long, var edg2: Long) {}

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

    val DATASET_PATH="./dataset/test.txt";

    //Import Dataset
    val importDataset = new ImportDataset()
    val graph = importDataset.ImportGraph(sc,DATASET_PATH)

    //Import edges file
    val dataset = importDataset.importTxt(sc, DATASET_PATH).map(item => item.split(" "))

    //Calculate common neighbors for each node
    val joinEdges = new JoinEdges()
    val commonNeighbors = joinEdges.getCommonNeighbors(ss, dataset)

    //Calculate weights for each edge
    val calculateWeights= new AssignWeigts()
    val weights = calculateWeights.ComputeWeight(commonNeighbors)

    weights.foreach(println)
  }
}

