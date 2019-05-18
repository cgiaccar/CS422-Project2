package cubeoperator

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import java.io._

object Main {
  def main(args: Array[String]) {
    val reducers = 10

    val inputFile= "../lineorder_small.tbl"
    val input = new File(getClass.getResource(inputFile).getFile).getPath

    val sparkConf = new SparkConf().setAppName("CS422-Project2").setMaster("local[16]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .load(input)

    val rdd = df.rdd

    val schema = df.schema.toList.map(x => x.name)

    val dataset = new Dataset(rdd, schema)

    val cb = new CubeOperator(reducers)

    var groupingList = List("lo_suppkey","lo_shipmode","lo_orderdate")

    val t1Naive = System.nanoTime
    //val res = cb.cube(dataset, groupingList, "lo_supplycost", "SUM")
    val res = cb.cube_naive(dataset, groupingList, "lo_supplycost", "SUM")
    val naiveSize = res.count()
    val t2Naive = System.nanoTime
    println("Naive count: " + naiveSize)
    println("Naive time: " + (t2Naive-t1Naive)/(Math.pow(10,9)))
    /*
       The above call corresponds to the query:
       SELECT lo_suppkey, lo_shipmode, lo_orderdate, SUM (lo_supplycost)
       FROM LINEORDER
       CUBE BY lo_suppkey, lo_shipmode, lo_orderdate
     */


    //Perform the same query using SparkSQL
    val t1SQL = System.nanoTime
    val q1 = df.cube("lo_suppkey","lo_shipmode","lo_orderdate")
    .agg(sum("lo_supplycost") as "sum supplycost")
    val SQLSize = q1.count()
    val t2SQL = System.nanoTime
    println("Count: " + SQLSize)
    println("SQL time: " + (t2SQL-t1SQL)/(Math.pow(10,9)))
    //q1.show(q1.count().toInt)
  }
}