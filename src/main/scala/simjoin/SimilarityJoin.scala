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
    rdd = dataset.getRDD().map(r => r.getString(attrIndex))
    val anchors : Array[String] = rdd.takeSample(false, numAnchors)
    val partitions: Array[RDD[String]] = partitioning(anchors)
    (for(par <- partitions) yield computeJoin(par)).reduce(_ union _)
  }


  def partitioning(anchors: Array[String]): Array[RDD[String]] = {
    val column : RDD[String] = rdd

    val distancesToAnchors : RDD[(String, List[Int])] = column.map(//list of distances from a point to each anchors
      element => (element, computeDistances(element, anchors))
    )
    for ((_,i) <- anchors.zipWithIndex) yield distancesToAnchors.filter(el => keepInPartition(el._2,i)).map(_._1)//new Array[RDD[String]](numAnchors)

    /*closestAnchors.foreach{case (name:String, list:List[Int]) => {

      val (anchorIndex, closestDistance) = list.zipWithIndex.min

      for((a,i) <- list.zipWithIndex) {
        if(a <= closestDistance + 2 * distThreshold)
          clusters(i) = name :: clusters(i)
      }
      clusters(anchorIndex) = name :: clusters(anchorIndex)
    }}*/

    //val clusters = new Array[List[String]](numAnchors)
    //closestAnchors.foreach(el => clusters(el._2) + el._1) //(el:String, num:Int) => clusters(num) + el )


  }

  def keepInPartition(list: List[Int], i: Int): Boolean = {
    val (anchorIndex, closestDistance) = list.zipWithIndex.min
    if (anchorIndex == i)
      return true

    //for((a,i) <- list.zipWithIndex) {
      if(list(i) <= closestDistance + 2 * distThreshold)
        return true//clusters(i) = name :: clusters(i)

    //}
    //clusters(anchorIndex) = name :: clusters(anchorIndex)

    return false
  }
  def computeDistances (element: String, anchors: Array[String]): List[Int] = {
    (for (a <- anchors) yield {
      Distance.distance(a, element)
    }).toList
    //.reduce((a1, a2) => if (a1._1 < a2._1) a1 else a2) //keep the index of the anchor
  }

  def computeJoin (partition: RDD[String]): RDD[(String, String)] = {
    /*for (el1 <- partition; el2 <- partition) yield {
      if (Distance.distance(el1, el2) <= distThreshold) (el1, el2)
    }*/
    //val test : RDD[(String, String)] = partition.map(s => (s, partition)).map{case (s1, rdd1) => rdd1.map(s2 => (s1, s2))}
    val cartesianProduct: RDD[(String, String)] = partition.cartesian(partition)

    cartesianProduct.filter{case (s1:String, s2:String) => if (s1 != s2 && Distance.distance(s1, s2) <= distThreshold) true else false}
  }

}


