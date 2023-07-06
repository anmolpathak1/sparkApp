package main.scala

import org.apache.spark.sql.SparkSession
import scala.io.Source

object SparkEntry extends App{

  val spark = SparkSession.
    builder().appName("firstSparkApp").master("local[*]").getOrCreate()    //to create a sparkSession

  val path = "src//main//resources//people.json"

  val df = spark.read.format("json").load(path)                          //job 0
  df.show()                                                                      //job 1

  import spark.implicits._
  df.printSchema()

  df.select('name).show()                                                 //job 2

  df.select('name,'age + 1).show()                                         //job3

  df.filter('age > 21).show()                                          //job4

  df.groupBy('age).count().show()                                          //job5 / 6

  //Action corresponds to a Job. And a job can have many stages. Task are parallel computation performed on partitions.

  df.createOrReplaceTempView("people")

  spark.sql("select * from people").show()                                //job7

  //create global temporary view

  df.createGlobalTempView("global_people")

  spark.sql("select * from global_temp.global_people").show()              //job8

  spark.newSession().sql("select * from global_temp.global_people").show()   //job 9

  scala.io.StdIn.readLine()

  spark.stop()                //
}
