package module.one.basic.statistics.data.types

import org.apache.spark.mllib.linalg.distributed.IndexedRow
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object EjemploSampling extends App{
  val conf = new SparkConf().setAppName("EjemploPairwiseCorrelations").setMaster("local")
  val sc = new SparkContext(conf)

  val elements: RDD[Vector] = sc.parallelize(Array(
    Vectors.dense(4,7,13),
    Vectors.dense(-2,8,4),
    Vectors.dense(3,-11,19)))

  val sample1 = elements.sample(withReplacement = false, fraction = 0.5, seed = 10L).collect()

  val sample2 = elements.sample(withReplacement = false, fraction = 0.5, seed = 7L).collect()

  val sample3 = elements.sample(withReplacement = false, fraction = 0.5, seed = 64L).collect()

  /*
    Random Split:

      - Can be performed on any RDD
      - Returns an array of RDDs
      - Weights for the split will be normalized if they do not add up to 1
      - Useful for splitting a data set into training, test and validation sets
   */

  val data = sc.parallelize(1 to 1000000)

  val splits = data.randomSplit(Array(0.6, 0.2, 0.2), seed = 13L)

  val training = splits(0)

  val test = splits(1)

  val validation = splits(2)

  val myArray = splits.map(_.count())

  var cadena: String = "[ "
  for(i <- 0 until myArray.length)
    cadena += myArray(i) + " "

  cadena += "]"

  println(cadena)

  /*
    Stratified Sampling

      - Can be performed on RDDs of key-value pairs
      - Think of keys as labels and values as an specific attribute
      - Two supported methods defined in PairRDDFunctions:
        - sampleByKey requires only one pass over the data and provides an expected sample size
        - sampleByKeyExact provides the exact sampling size with 99.99% confidence but requires
          significantly more resources
   */

  val rows: RDD[IndexedRow] = sc.parallelize(Array(
    IndexedRow(0, Vectors.dense(1,2)),
    IndexedRow(1, Vectors.dense(4,5)),
    IndexedRow(1, Vectors.dense(7,8))))

  val fractions: Map[Long, Double] = Map(0L -> 1, 1L -> 0.5)

  val approxSample = rows.map{
    case IndexedRow(index, vec) => (index, vec)
  }.sampleByKey(withReplacement = false, fractions, 9L)

  val a = approxSample.collect()

  cadena = "["
  for(i <- 0 until a.length)
    cadena += a(i) + " "

  cadena += "]"

  println(cadena)
}
