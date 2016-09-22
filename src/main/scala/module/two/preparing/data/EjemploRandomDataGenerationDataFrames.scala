package module.two.preparing.data

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.{rand, randn}

object EjemploRandomDataGenerationDataFrames extends App{

  val conf = new SparkConf().setAppName("EjemploRandomDataGenerationDataFrames").setMaster("local")
  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)

  val df = sqlContext.range(0, 10)

  df.select("id").withColumn("uniform", rand(10L)).withColumn("normal", randn(10L)).show()
}
