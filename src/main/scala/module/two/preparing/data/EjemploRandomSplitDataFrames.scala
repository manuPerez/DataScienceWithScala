package module.two.preparing.data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext

object EjemploRandomSplitDataFrames extends App {
  /*
    Random Split on DataFrames

      - Can be performed on any DataFrame
      - Returns an array of DataFrames
      - Weights for the split will be normalized if they do not add un to 1
      - Useful for splitting a data set into training, test and validation sets
   */

  val conf = new SparkConf().setAppName("EjemploRandomSplitDataFrames").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContesxt = new SQLContext(sc)

  val df = sqlContesxt
    .createDataFrame(Seq((1, 10), (1, 20), (2, 10), (2, 20), (2, 30), (3, 20), (3, 30))).toDF("key", "value")

  val dfSplit = df.randomSplit(weights = Array(0.3, 0.7), seed = 11L)

  dfSplit(0).show()
  dfSplit(1).show()
}
