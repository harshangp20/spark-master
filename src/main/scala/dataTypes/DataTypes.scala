package dataTypes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit}

object DataTypes extends App {

  val spark = SparkSession
    .builder()
    .config("spark.master","local")
    .appName("Spark Data Types")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read
    .option("inferSchema","true")
    .option("multiLine","True")
    .json("src/main/resources/data/movies.json")

//  insert New Value in DataFrames
  moviesDF.select(col("Title"), lit("69").as("Name"))//.show()

//  Boolean
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").filter(dramaFilter)//.show()

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))//.show()
  moviesWithGoodnessFlagsDF.where("good_movie") .show

}
