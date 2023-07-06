package main.scala.questions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType,StructType,StructField}
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

object question1 extends App{

  val data = List(Row(1,2),
                 Row(2,13),
                  Row(3,45),
                  Row(5,31),
                  Row(4,99)
                  )

  val spark = SparkSession.builder().appName("question1").master("local[*]").getOrCreate()
  import spark.implicits._

  val manualSchema = StructType(Array(StructField("key",IntegerType,true),
                                StructField("value",IntegerType,true)
                          ))
  val rdd = spark.sparkContext.parallelize(data)
  val df = spark.createDataFrame(rdd,manualSchema)

  val s = df.agg(sum("value").as("sum"))

  val df2 = df.crossJoin(s)
  val df3 = df2.withColumn("division",col("value")/col("sum"))
  df3.show(false)

  scala.io.StdIn.readLine()
  spark.stop()
}
