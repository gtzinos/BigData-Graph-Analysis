package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.GraphLoader
import org.apache.spark.sql.SparkSession

class ImportDataset extends Serializable {

  // Import dataset from csv as Dataframe
  def importDatasets(session: SparkSession, datasets: Array[String]) = {
    for (dataset: String <- datasets) {
      val fileName = getFileName(dataset)
      session.read.option("header", "true").csv(dataset).createOrReplaceTempView(fileName)
    }
  }

  // Returns file name from a path
  def getFileName(path: String) = {
    val paths: Array[String] = path.split("/")
    val fileName = paths(paths.length - 1).split('.')(0)

    fileName
  }

  // Import dataset from txt and returns it as RDD
  def importTxt(sc: SparkContext, filePath: String) = {
    sc.textFile(filePath)
  }

  //ImportDataset and return it as Graph using graphx
  def ImportGraph(sc: SparkContext, filePath: String, numberEdgePartitions: Int = 4) = {
    GraphLoader.edgeListFile(sc, filePath, true)
  }

}
