package main.scala.questions
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.lit

object mergeDemo extends App{

  val spark  = SparkSession.
    builder().
    appName("mergeDemo").
    master("local[*]").
    getOrCreate()

  val df1 =spark.read.format("csv").
    options(Map("sep" -> "|","header" ->"true")).load("C:\\Users\\patha\\OneDrive\\Documents\\players.txt").
    withColumn("gender",lit("not know"))

  val df2 = spark.read.format("csv").
    options(Map("sep" -> "|","header" -> "true")).load("C:\\Users\\patha\\OneDrive\\Documents\\player2.txt")

  df1.show()
  df2.show()

  val df3 = df1 union df2
  df3.show()
}
