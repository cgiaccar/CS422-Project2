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
    //For debug
    //******************************************************************************************************************
    //println("Anchors:")
    //anchors.foreach(println)
    //******************************************************************************************************************
    val partitions: Array[RDD[String]] = partitioning(anchors)

    ((for(par <- partitions) yield computeJoin(par)).reduce(_ union _)).distinct()
  }


  def partitioning(anchors: Array[String]): Array[RDD[String]] = {
    val column : RDD[String] = rdd

    val distancesToAnchors : RDD[(String, List[Int])] = column.map(//list of distances from a point to each anchors
      element => (element, computeDistances(element, anchors))
    ).persist()

    // For debug
    //******************************************************************************************************************
    //val d1 = distancesToAnchors.collect()
    //println("Distances to anchors:")
    //d1.foreach(println)
    //******************************************************************************************************************

    val res = for ((_,i) <- anchors.zipWithIndex) yield distancesToAnchors.filter(el => keepInPartition(el._2, i)).map(_._1).persist()

    // For debug
    //******************************************************************************************************************
    //val innerPartition = for ((_,i) <- anchors.zipWithIndex) yield distancesToAnchors.filter(el => keepInInnerPartition(el._2, i)).map(_._1)

    //val outerPartition = for ((_,i) <- anchors.zipWithIndex) yield distancesToAnchors.filter(el => keepInOuterPartition(el._2, i)).map(_._1)

    //for ((anchors,i) <- anchors.zipWithIndex) {
    //  println("Anchors: " + anchors)
    //  println("InnerPartition:")
    //  innerPartition(i).collect().foreach(println)
    //  println("OuterPartition:")
    //  outerPartition(i).collect().foreach(println)
    //}
    //******************************************************************************************************************

    res
  }

  def keepInPartition(list: List[Int], i: Int): Boolean = {
    val (closestDistance, anchorIndex) = list.zipWithIndex.min

    if (anchorIndex == i)
      return true

    if (list(i) <= closestDistance + 2 * distThreshold)
      return true

    false
  }

  // For debug
  //********************************************************************************************************************
  def keepInInnerPartition(list: List[Int], i: Int): Boolean = {
    val (_, anchorIndex) = list.zipWithIndex.min

    if (anchorIndex == i)
      return true

    false
  }

  def keepInOuterPartition(list: List[Int], i: Int): Boolean = {
    val closestDistance = list.min

    if (list(i) <= closestDistance + 2 * distThreshold)
      return true

    false
  }
  //********************************************************************************************************************

  def computeDistances (element: String, anchors: Array[String]): List[Int] = {
    (for (a <- anchors) yield {
      Distance.distance(element, a)
    }).toList
  }

  def computeJoin (partition: RDD[String]): RDD[(String, String)] = {
    // For debug
    //******************************************************************************************************************
    //println("Partition :")
    //partition.collect().foreach(println)
    //******************************************************************************************************************
    println("partition size : " + partition.count())

    val cartesianProduct: RDD[(String, String)] = partition.cartesian(partition).persist()

    val res = cartesianProduct.filter{case (s1:String, s2:String) => if (s1 != s2 && Distance.distance(s1, s2) <= distThreshold) true else false}
    // For debug
    //******************************************************************************************************************
    //val resP = res.collect()
    //println("Join result: ")
    //resP.foreach(println)
    //******************************************************************************************************************
    res
  }

}


