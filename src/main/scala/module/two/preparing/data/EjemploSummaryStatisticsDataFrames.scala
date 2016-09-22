package module.two.preparing.data

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object EjemploSummaryStatisticsDataFrames extends App{
  /*
    Summary Statistics for DataFrames

      - Column summary statistics for DataFrames are available through DataFrame's describe() method
      - It returns another DataFrame, which contains column-wise results for:
        - min, max
        - mean, stddev
        - count
      - Column summary statistics can also be computed through DataFrame's groupBy() and agg() methods,
        but stddev is not supported
      - It also returns another DataFrame with the results
   */

  val conf = new SparkConf().setAppName("EjemploSummaryStatisticsDataFrames").setMaster("local")
  val sc = new SparkContext(conf)
  val sQLContext = new SQLContext(sc)

  import sQLContext.implicits._

  case class Record(desc: String, value1: Int, value2: Double)

  val recDF = sc.parallelize(Array(
    Record("first", 1, 3.7),
    Record("second", -2, 2.1),
    Record("third", 6, 0.7))).toDF()

  val recStats = recDF.describe()
  recStats.show()

  //Fetching Results from DataFrame
  val stddevDF = recStats.filter("summary = 'stddev'").first()

  println(stddevDF)

  val stddevArray = stddevDF.toSeq.toArray

  var cadena: String = ""
  for(i <- 0 until stddevArray.length)
    cadena += stddevArray(i) + " "

  println("stddevArray: \n" + cadena)

  val stddevArray2 = stddevArray.drop(1).map(_.toString.toDouble)

  cadena = ""
  for(i <- 0 until stddevArray2.length)
    cadena += stddevArray2(i) + " "

  println("stddevArray2: \n" + cadena)

  val stddevArray3 = recStats.select("value1").map(s => s(0).toString.toDouble).collect()

  cadena = ""
  for(i <- 0 until stddevArray3.length)
    cadena += stddevArray3(i) + " "

  println("stddevArray3: \n" + cadena)

  val a = recDF.groupBy().agg(Map("value1" -> "min", "value1" -> "max"))

  println(a)

  val b = recDF.groupBy().agg(Map("value1" -> "min", "value2" -> "min"))

  println(b)

  import org.apache.spark.sql.functions._
  val c = recDF.groupBy().agg(min("value1"), min("value2"))

  cadena = ""
  for(i <- 0 until c.columns.length)
    cadena += c.columns(i) + " "

  println("c: \n" + cadena)

  val d = c.first().toSeq.toArray.map(_.toString.toDouble)

  cadena = ""
  for(i <- 0 until d.length)
    cadena += d(i) + " "

  println("c: \n" + cadena)

  val recDFStat = recDF.stat

  println("corr: " + recDFStat.corr("value1", "value2"))

  println("cov: " + recDFStat.cov("value1", "value2"))

  recDFStat.freqItems(Seq("value1"), 0.3).show()

}
