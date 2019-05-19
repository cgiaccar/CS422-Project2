package cubeoperator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

class CubeOperator(reducers: Int) {

  /*
 * This method gets as input one dataset, the grouping attributes of the cube (CUBE BY clause)
 * the attribute on which the aggregation is performed
 * and the aggregate function (it has to be one of "COUNT", "SUM", "MIN", "MAX", "AVG")
 * and returns an RDD with the result in the form of <key = string, value = double> pairs.
 * The key is used to uniquely identify a group that corresponds to a certain combination of attribute values.
 * You are free to do that following your own naming convention.
 * The value is the aggregation result.
 * You are not allowed to change the definition of this function or the names of the aggregate functions.
 * */
  def cube(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {

    val rdd: RDD[Row] = dataset.getRDD().persist()
    val schema: List[String] = dataset.getSchema()

    val indexes: List[Int] = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg: Int = schema.indexOf(aggAttribute)
    if (agg == "AVG") {
      val topCuboid: RDD[(List[(String, Any)], (Double, Int))] = rdd.map(e => (indexes.map(i => (schema(i), e.get(i))), (e.getInt(indexAgg).toDouble, 1))).reduceByKey((d1, d2) => (d1._1 + d2._1, d1._2 + d2._2), reducers).persist()
      val cube: RDD[(String, Double)] = topCuboid.flatMap { e =>
        val partialCubeLattices: List[List[Any]] = (for (i <- 0 to e._1.size) yield e._1.combinations(i)).flatten.toList
        for (r <- partialCubeLattices) yield (r.toString, e._2)
      }.reduceByKey((d1,d2) => (d1._1 + d2._1, d1._2 + d2._2), reducers).map{case (k, v) => (k, v._1/v._2)}
      cube
    } else {
      val topCuboid: RDD[(List[(String, Any)], Double)] = agg match {
        case "COUNT" =>
         rdd.map(e => (indexes.map(i => (schema(i), e.get(i))), 1.0)).reduceByKey(_+_, reducers).persist()
        case "SUM" =>
          rdd.map(e => (indexes.map(i => (schema(i), e.get(i))), e.getInt(indexAgg).toDouble)).reduceByKey(_+_, reducers).persist()
        case "MIN" =>
          rdd.map(e => (indexes.map(i => (schema(i), e.get(i))), e.getInt(indexAgg).toDouble)).reduceByKey((d1,d2) => math.min(d1,d2), reducers).persist()
        case "MAX" =>
          rdd.map(e => (indexes.map(i => (schema(i), e.get(i))), e.getInt(indexAgg).toDouble)).reduceByKey((d1,d2) => math.max(d1,d2), reducers).persist()
      }
      val cube: RDD[(String, Double)] = topCuboid.flatMap { e =>
        val partialCubeLattices: List[List[Any]] = (for (i <- 0 to e._1.size) yield e._1.combinations(i)).flatten.toList
        for (r <- partialCubeLattices) yield (r.toString, e._2)
      }.reduceByKey((d1,d2) => {
        agg match {
          case "COUNT" =>
            d1 + d2
          case "SUM" =>
            d1 + d2
          case "MIN" =>
            math.min(d1, d2)
          case "MAX" =>
            math.max(d1, d2)
        }
      }, reducers)
      cube
    }
  }

  def cube_naive(dataset: Dataset, groupingAttributes: List[String], aggAttribute: String, agg: String): RDD[(String, Double)] = {
    val rdd: RDD[Row] = dataset.getRDD().persist()
    val schema: List[String] = dataset.getSchema()

    val indexes: List[Int] = groupingAttributes.map(x => schema.indexOf(x))
    val indexAgg: Int = schema.indexOf(aggAttribute)


    val cube_lattice: List[List[Int]] = (for (i <- 0 to indexes.size) yield indexes.combinations(i)).flatten.toList

    val result: RDD[(String, Double)] = agg match {
      case "COUNT" =>
        val mapping: RDD[(String, Double)] = rdd.flatMap {e =>
          for (r <- cube_lattice) yield (r.map(i => e.get(i)).toString, 1.0)
        }.persist()
        mapping.reduceByKey(_+_)

      case "SUM" =>
        val mapping: RDD[(String, Double)] = rdd.flatMap {e =>
          for (r <- cube_lattice) yield (r.map(i => schema(i) + ":" + e.get(i)).toString, e.getInt(indexAgg).toDouble)
        }.persist()
        mapping.reduceByKey(_+_)

      case "MIN" =>
        val mapping: RDD[(String, Double)] = rdd.flatMap {e =>
          for (r <- cube_lattice) yield (r.map(i => e.get(i)).toString, e.getInt(indexAgg).toDouble)
        }.persist()
        mapping.reduceByKey((d1, d2) => math.min(d1,d2))

      case "MAX" =>
        val mapping: RDD[(String, Double)] = rdd.flatMap {e =>
          for (r <- cube_lattice) yield (r.map(i => e.get(i)).toString, e.getInt(indexAgg).toDouble)
        }.persist()
        mapping.reduceByKey((d1, d2) => math.max(d1,d2))

      case "AVG" =>
        val mapping: RDD[(String, (Double, Int))] = rdd.flatMap {e =>
          for (r <- cube_lattice) yield (r.map(i => e.get(i)).toString, (e.getInt(indexAgg).toDouble, 1))
        }.persist()
         mapping.reduceByKey((d1, d2) => (d1._1 + d2._1, d1._2 + d2._2)).map{case (k, v) => (k, v._1/v._2)}
    }

    result
  }
}
