package utils

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, PartitionStrategy}
import org.apache.spark.sql.SparkSession

  class ImportDataset {

      def getFileName(path: String) = {
        val paths: Array[String] = path.split("/")
        val fileName = paths(paths.length-1).split('.')(0)

        fileName
      }

      def importDatasets(session: SparkSession, datasets: Array[String]) = {
        for(dataset: String <- datasets) {
          val fileName = getFileName(dataset)
          session.read.option("header", "true").csv(dataset).createOrReplaceTempView(fileName)
        }
      }

      def importTxt(sc: SparkContext, filePath: String) = {
        sc.textFile(filePath)
      }

      def ImportGraph(sc: SparkContext  , filePath: String, numberEdgePartitions: Int = 4) = {

          GraphLoader.edgeListFile(sc, filePath, true)

      }

    }
