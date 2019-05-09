package simjoin

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SimilarityJoin(numAnchors: Int, distThreshold:Int) extends java.io.Serializable {
  val logger = LoggerFactory.getLogger("SimilarityJoin")
  var rdd: RDD[String] = null
  
  /*
   * this method gets as input a dataset and the index of an attribute
   * of the dataset, and returns the result of the similarity self join on
   * that attribute.
   * */  
  def similarity_join(dataset: Dataset, attrIndex: Int) : RDD[(String, String)] = {
    
    null
  }

  def extractRandom(numAnchors: Int, dataset: Dataset): Array[Row] = {
    dataset.getRDD().takeSample(false, numAnchors)
  }

  def partitioning(dataset: Dataset, attrIndex: Int, anchorPoints: Array[Row]): String = {
    val column : RDD[String] = dataset.getRDD()(attrIndex)
    val anchors: Array[String] = anchorPoints()(attrIndex)

    val closestAnchors : RDD[Int] = column.map(        //list of closest anchors to each element of the column
                      element => assist(anchorPoints, element, anchors)
                      )
    val tuples = column.zip(closestAnchors)
    val clusters = new Array[List[String]](numAnchors)
    tuples.foreach(el => clusters(el._2) + el._1) //(el:String, num:Int) => clusters(num) + el )


  }

  def assist (anchorPoints: Array[Row], element: String, anchors: Array[String]): Int = {
    {for (i <- 0 to anchorPoints.size) yield {
      (distance(anchors(i), element), i)
    }
    }.reduce((a1, a2) => if (a1._1 < a2._1) a1._2 else a2._2)
  }


  def distance(first: String, second: String): Int = {
    val matrix  = Array.ofDim[Int](first.length(), second.length())
    for(i <- 0 to first.length()) {
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

}

