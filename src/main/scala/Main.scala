import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SQLContext, SparkSession}
import utils._


 abstract class merger(var nei1: Long, var nei2: Array[Long], var edg1: Long, var edg2: Long) {}

object Main {



  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  def main(args: Array[String]): Unit = {
    //Session configuration
    val conf = new SparkConf().set("spark.driver.maxResultSize", "8g")
    // Create the spark session first
    val ss = SparkSession.builder().config(conf).master("local").appName("tfidfApp").getOrCreate()
    //Import implicits
    import ss.implicits._
    //Spark context
    val sc = ss.sparkContext

    val DATASET_PATH="./dataset/test.txt";

    val importDataset = new ImportDataset()
    val filterDataset = new FilterEdges()
    val aw= new AssignWeigts()

    val graph = importDataset.ImportGraph(sc,DATASET_PATH)

    val allEdges = graph.edges.map(item => if(item.srcId > item.dstId) (item.dstId: Long, item.srcId: Long) else (item.srcId: Long,item.dstId: Long))

    //val neighbors = graph.collectNeighbors(EdgeDirection.Either).groupByKey().mapValues(l3 => l3.flatMap(l4=>l4.map(l5 => l5._1):Seq[Long]).toSeq: Seq[Long])

    val tests = importDataset.importTxt(sc, DATASET_PATH).map(item => item.split(" "))

    val test1  = tests.map(item => (item(0).toLong, item(1).toLong))
    val test2 = tests.map(item => (item(1).toLong, item(0).toLong))

    val test3 = test1.groupByKey()
    val test4 = test2.groupByKey()

    val neighbors = test3.union(test4).groupByKey().mapValues(item => item.flatten.toSeq)


    neighbors.foreach(println)
    neighbors.toDF().createOrReplaceTempView("neighbors")
    allEdges.toDF().createOrReplaceTempView("allEdges")

    allEdges.foreach(println)

    val ralledges = allEdges.map(item => (item._2, item._1))

    val merge = ss.sql("" +
      " Select DISTINCT edg._1 as sourceId, edg._2 as targetId , nei._2 as sourceNeighbors, nei2._2 as targetNeighbors" +
      " from neighbors nei, allEdges edg, neighbors nei2"+
      " where nei._1 = edg._1 and nei2._1 = edg._2"
      )
      //.createOrReplaceTempView("joinedNeighbors")
        .rdd
        .map(row => (row(0), row(1), row(2), row(3)))
        .foreach(println)

    



    for (al <- ralledges) {
      print("chocobloko")
      val graphs = importDataset.ImportGraph(sc,DATASET_PATH)
      println(graphs.triangleCount())
    }

    //.groupByKey().foreach(println)

    neighbors.foreach(println)

    allEdges.foreach(println)

    println()

    //neighbors.join(allEdges).foreach(println)

    ss.createDataFrame(allEdges).createOrReplaceTempView("allEdges")

    ss.createDataFrame(neighbors).createOrReplaceTempView("neighbors")

    /* ss.sql("Select ae._1 as esrc , ae._2 as edest, ne._2 as nei from (" +
       " select ae._1 as l2_esrc , ae._2 as l2_edest, ne._2 as nei" +
       " from allEdges ae, neighbors ne " +
       " where ae._1 = ne._1 or ae._2 = ne._1" +
       " group by ae._1").show() */

    val joined = ss.sql("Select ae._1 as esrc , ae._2 as edest, ne._2 as nei" +
      " from allEdges ae, neighbors ne " +
      " where ae._1 = ne._1 or ae._2 = ne._1")

    //joined.groupByKey(StringToColumn("re"))//.foreach(println)

    for (edge <- allEdges) {
      graph.triplets.collect {
        case t if t.srcId == edge._1 && t.srcId == edge._2 => t.dstId
      }
    }


    /*

        select e.empID, fname, lname, title, dept, projectIDCount
        from
        (
          select empID, count(projectID) as projectIDCount
          from employees E left join projects P on E.empID = P.projLeader
        group by empID
        ) idList
        inner join employees e on idList.empID = e.empID
    */
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

