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
    rdd = dataset.getRDD().map(r => r.getString(attrIndex)).persist()

    val anchors : Array[String] = rdd.takeSample(false, numAnchors)

    val partitions: Array[RDD[String]] = partitioning(anchors)

    (for(par <- partitions) yield computeJoin(par)).reduce(_ union _).distinct()
  }


  def partitioning(anchors: Array[String]): Array[RDD[String]] = {
    val distancesToAnchors : RDD[(String, List[Int])] = rdd.map(//list of distances from a point to each anchors
      element => (element, computeDistances(element, anchors))
    ).persist()

    for ((_,i) <- anchors.zipWithIndex) yield distancesToAnchors.filter(el => keepInPartition(el._2, i)).map(_._1).persist()
  }

  def keepInPartition(list: List[Int], i: Int): Boolean = {
    val (closestDistance, anchorIndex) = list.zipWithIndex.min

    if (anchorIndex == i)
      return true

    if (list(i) <= closestDistance + 2 * distThreshold)
      return true

    false
  }

  def computeDistances (element: String, anchors: Array[String]): List[Int] = {
    (for (a <- anchors) yield {
      edit_distance(element, a)
    }).toList
  }

  def computeJoin (partition: RDD[String]): RDD[(String, String)] = {
    val cartesianProduct: RDD[(String, String)] = partition.cartesian(partition).persist()

    cartesianProduct.filter{case (s1:String, s2:String) => if (s1 != s2 && edit_distance(s1, s2) <= distThreshold) true else false}
  }

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
}