package main.scala

import org.apache.spark.sql.SparkSession

object multiDelimter extends App{

  val spark = SparkSession.
    builder().
    appName("mutliDelimiter").
    master("local[*]").
    getOrCreate()

  import spark.implicits._

  val file = spark.read.textFile("C:\\Users\\patha\\OneDrive\\Documents\\multiDelimiter.txt").rdd

  val fileValue = file.collect()

  fileValue.foreach(println)
  val file2 = file.map(x => x.mkString).map(x => x(0).asInstanceOf[String].split("~|")).toDF("str")

  file2.show()

//  val df = spark.read.format("csv").options(Map("header" -> "true","sep" -> "~|"))
//    .load("C:\\Users\\patha\\OneDrive\\Documents\\multiDelimiter.txt")


//  scala.io.StdIn.readLine()
}
