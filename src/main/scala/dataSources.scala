package main.scala
import org.apache.spark.sql.SparkSession

//task 1 - read files using SQL query in spark without using DataFrame

object dataSources extends App{


  val spark = SparkSession.builder().appName("dataSources").master("local[*]")getOrCreate()
  val df = spark.sql("select * from  json .`C:\\Users\\patha\\IdeaProjects\\projectCode\\sparkApp\\src\\main\\resources\\people.json`") //.show()

  val df1 =spark.read.parquet("src/main/resources/dir1/file1.parquet","src/main/resources/dir1/dir2/file2.parquet")

  df1.show()

  val df2 = spark.read.format("parquet")
    .option("pathGlobFilter","*.parquet").load("src/main/resources/dir1")
  df2.show()

  //to stop app by keypress
  scala.io.StdIn.readLine
  spark.stop()
}
