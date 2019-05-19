package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

import org.apache.spark.rdd.RDD

object Main {

  /*def compareRDD(c1: RDD[(String, Double)], c2: RDD[(String, Double)]): Boolean = {
    val t: RDD[Boolean] = c1.union(c2).groupByKey().map(e => if(e._2.size == 2) {e._2.toList(0) == e._2.toList(1)} else false)
    t.reduce(_&&_)
  }*/

  def main(args: Array[String]) {
    //val reducers = 10

    //val inputFile= "../lineorder_small.tbl"
    val inputFile="/user/cs422-group20/testEx2/lineorder_small.tbl"
    //val input = new File(getClass.getResource(inputFile).getFile).getPath

    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(inputFile)//.load(input)

    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)
    val groupingLists = List(List("lo_suppkey","lo_shipmode"), List("lo_suppkey","lo_shipmode","lo_orderdate"), List("lo_suppkey","lo_shipmode","lo_orderdate", "lo_quantity"))

    for (reducers <- List(1, 5, 10, 15, 20)) {
      for (groupingList <- groupingLists) {
        val cb = new CubeOperator(reducers)

        //var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")

        val t1Naive = System.nanoTime
        val resNaive = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")
        val naiveSize = resNaive.count()
        val t2Naive = System.nanoTime
        println("Number of reducers: " + reducers)
        println("groupingList: " + groupingList)
        println("Naive count: " + naiveSize)
        println("Naive time: " + (t2Naive-t1Naive)/(Math.pow(10,9)))

        val t1Cube = System.nanoTime
        val resCube = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
        val cubeSize = resCube.count()
        val t2Cube = System.nanoTime
        println("Cube count: " + cubeSize)
        println("Cube time: " + (t2Cube-t1Cube)/(Math.pow(10,9)))

        /*
           The above call corresponds to the query:
           SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
           FROM LINEORDER
           CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
         */


        //Perform the same query using SparkSQL
        /*val t1SQL = System.nanoTime
        val col1 = "lo_suppkey"
        val col2 = groupingList._2
        val q1 = df.cube(col1, col2)//"lo_suppkey","lo_shipmode","lo_orderdate")
          .agg(sum("lo_supplycost") as "sum supplycost")
        val SQLSize = q1.count()
        val t2SQL = System.nanoTime
        println("Count: " + SQLSize)
        println("SQL time: " + (t2SQL-t1SQL)/(Math.pow(10,9)))*/

        println("Sizes match:" + (naiveSize == cubeSize))
      }
    }
  }
}