package module.two.preparing.data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object EjemploSamplingDataFrames extends App{
  /*
    Sampling on DataFrames

      - Can be performed on any DataFrame
      - Returns a sampled subset of a DataFrame
      - Sampling with or without repacement
      - Fraction: expected fraction of rows to generate
      - Can be used on bootstrapping procedures
   */

  val conf = new SparkConf().setAppName("EjemploSamplingDataFrames").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContesxt = new SQLContext(sc)

  val df = sqlContesxt
    .createDataFrame(Seq((1, 10), (1, 20), (2, 10), (2, 20), (2, 30), (3, 20), (3, 30))).toDF("key", "value")

  val dfSampled = df.sample(withReplacement = false, fraction = 0.3, seed = 11L)

  dfSampled.show()

  /*
    Stratified Sampling on DataFrames

      - Can be performed on any DataFrame
      - Any column may work as key
      - Without replacement
      - Fraction: specified by key
      - Available as sampleBy function in DataFrameStatFunctions
   */

  val dfStrat = df.stat.sampleBy(col = "key", fractions = Map(1 -> 0.7, 2 -> 0.7, 3 -> 0.7), seed = 11L)

  dfStrat.show()
}
