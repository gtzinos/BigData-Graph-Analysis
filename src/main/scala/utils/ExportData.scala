package utils

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVWriter
import java.io.{File, StringWriter}

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import scala.collection.JavaConversions._

class ExportData {

  def deleteFile(sc: SparkContext, filePath: String) = {
    val fs=FileSystem.get(sc.hadoopConfiguration)
    if(fs.exists(new Path(filePath)))
      fs.delete(new Path(filePath),true)
  }

  def ExportEdgesToCsv(sc: SparkContext, edges: RDD[Edge[Long]], hasWeights: Boolean = false, filePath: String = "./exports/exports.csv", delimiter: String = " ") = {
    deleteFile(sc, filePath)

    edges.repartition(1).map(row => {
      if (hasWeights)
        row.srcId + delimiter + row.dstId
      else
        row.dstId + delimiter + row.dstId + delimiter + row.attr
    }).saveAsTextFile(filePath)
  }
}
