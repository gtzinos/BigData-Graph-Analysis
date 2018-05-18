package utils

import org.apache.spark.graphx.Edge
import org.apache.spark.rdd.RDD
import au.com.bytecode.opencsv.CSVWriter
import java.io.StringWriter
import scala.collection.JavaConversions._

class ExportData {

  def ExportEdgesToCsv(edges: RDD[Edge[Long]], hasWeights: Boolean = false, filePath: String = "./exports/exports.csv", delimiter: String = " ") = {
    edges.repartition(1).map(row => {
      if (hasWeights)
        row.srcId + delimiter + row.dstId
      else
        row.dstId + delimiter + row.dstId + delimiter + row.attr
    }).saveAsTextFile(filePath)
  }
}
