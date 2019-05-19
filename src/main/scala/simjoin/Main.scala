package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLContext, Row, DataFrame}
import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

import scala.io.Source
import java.io._

object Main {

  def main(args: Array[String]) {
    def edit_distance(first: String, second: String): Int = {
      val matrix  = Array.ofDim[Int](first.length()+1, second.length()+1)
      for(i <- 1 to first.length()) {
        matrix(i)(0) = i
      }
      for(j <- 1 to second.length()) {
        matrix(0)(j) = j
      }

      for(i <- 1 to first.length()) {
        for (j <- 1 to second.length()) {
          if (first.charAt(i-1) != second.charAt(j-1)) {
            val k = math.min(
              math.min(
                matrix(i)(j - 1),
                matrix(i - 1)(j)),
              matrix(i - 1)(j - 1)
            )
            matrix(i)(j) = k + 1
          }
          else matrix(i)(j) = matrix(i-1)(j-1)
        }
      }

      matrix(first.length())(second.length())

    }


    val inputFile="/user/cs422-group20/testEx2/dblp_small.csv"
    //val inputFile="../dblp_small.csv"
    //val numAnchors = 12
    val distanceThreshold = 2
    val attrIndex = 0

    //val input = new File(getClass.getResource(inputFile).getFile).getPath
    val sparkConf = new SparkConf().setAppName("CS422-Project2")//.setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    val sqlContext = new org.apache.spark.sql.SQLContext(ctx)

    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "false")//.option("header", "true") header->false because the given test files don't have a header
      .option("inferSchema", "true")
      .option("delimiter", ",")
      /*.load(input)*/.load(inputFile)

    val rdd = df.rdd
    val schema = df.schema.toList.map(x => x.name)
    val dataset = new Dataset(rdd, schema)

    // cartesian
    val t1Cartesian = System.nanoTime
    val cartesian = rdd.map(x => (x(attrIndex), x)).cartesian(rdd.map(x => (x(attrIndex), x)))
      .filter(x => (x._1._2(attrIndex).toString() != x._2._2(attrIndex).toString() && edit_distance(x._1._2(attrIndex).toString(), x._2._2(attrIndex).toString()) <= distanceThreshold))
    val cartesianSize = cartesian.count
    println("Cartesian count: " + cartesianSize)
    val t2Cartesian = System.nanoTime
    println("Cartesian time: " + (t2Cartesian-t1Cartesian)/(Math.pow(10,9)))

    val numAnchorsList: List[Int] = List(2, 4, 6, 8, 10, 12, 14, 16, 32, 64)
    for (numAnchors <- numAnchorsList) {
      val t1 = System.nanoTime
      val sj = new SimilarityJoin(numAnchors, distanceThreshold)
      val res = sj.similarity_join(dataset, attrIndex)

      val resultSize = res.count
      val t2 = System.nanoTime
      println("Num anchors: " + numAnchors)
      println("SimJoin count: " + resultSize)

      println("SimJoin time: " + (t2-t1)/(Math.pow(10,9)))
      assert(resultSize == cartesianSize)
    }
  }
}