package main.scala
import org.apache.spark.sql.SparkSession


object DataSetDemo extends App{

  case class Person(name : String,age : Long)

  val spark = SparkSession.builder().appName("datasets").master("local[*]").getOrCreate()

  import spark.implicits._

  val caseClassDS = Seq(Person("Andy",23)).toDS()
  caseClassDS.show()


  val primitiveDS = Seq(1,2,3).toDS()
  primitiveDS.map(_ + 1).collect()


  //converting dataFrames to Datasets

  val path = "src//main//resources//people.json"
  val peopleDS = spark.read.format("json").load(path).as[Person]
  peopleDS.show()

  scala.io.StdIn.readLine()



}
