package dataTypes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object DataTypes extends App {

  val spark = SparkSession
    .builder()
    .config("spark.master", "local")
    .appName("Spark Data Types")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .option("multiLine", "True")
    .json("src/main/resources/data/movies.json")

  //  insert New Value in DataFrames
  moviesDF.select(col("Title"), lit("69").as("Name")) //.show()

  //  Boolean
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").filter(dramaFilter) //.show()

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie")) //.show()
  moviesWithGoodnessFlagsDF.where("good_movie") //.show

  moviesWithGoodnessFlagsDF.where(not(col("good_movie"))) //.show

  val moviesAvgRatingsDF = moviesDF
    .select(col("Title"), (col("IMDB_Rating") + col("Rotten_Tomatoes_Rating") / 10) / 2)

  moviesAvgRatingsDF //.show()

  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  val carsDF = spark
    .read
    .option("inferSchema", "true")
    .option("multiLine", "true")
    .json("src/main/resources/data/cars.json")

  carsDF.select(initcap(col("Name"))).show()

  carsDF
    .select(col("Horsepower")
      , col("Displacement"))
    .where(col("Name")
      .contains("volkswagen")).show()

  val regexString = "volkswagen|vw"

  var vwDF = carsDF.select(

    col("Name")
    , regexp_extract(col("Name"), regexString, 0).as("complete_name")

  ).where(col("complete_name") =!= "").drop("complete_name")

  vwDF.show()


}
