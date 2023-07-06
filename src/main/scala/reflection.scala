package main.scala

import org.apache.spark.sql.SparkSession

object reflection extends App{

  val spark = SparkSession.builder().master("local[*]").appName("using reflection").getOrCreate()
  import spark.implicits._

  val path  = "src/main/resources/people.txt"
  val personDf = spark.sparkContext.textFile(path).
     map(_.split(","))
    .map(attributes => Person(attributes(0), attributes(1).trim.toInt))
    .toDF()

  personDf.createOrReplaceTempView("people")

  personDf.map(row => " Name : " + row(0)).show()

  personDf.map(row =>   row.getAs[Int]("age") + (row.getAs[String]("name"))).show()
  personDf.map(row =>   row.getAs[String]("name") + row.getAs[Int]("age")).show()



}

case class Person(name : String, age : Int)