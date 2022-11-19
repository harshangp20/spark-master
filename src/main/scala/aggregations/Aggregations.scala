package aggregations

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Aggregations extends App {

  val spark =
    SparkSession.builder()
      .appName("Aggregations and Grouping")
      .config("spark.master","local")
      .getOrCreate()

  val moviesDF =
    spark.read
      .option("inferSchema","true")
      .option("multiline","true")
      .json("src/main/resources/data/movies.json")

//  moviesDF.show()

  val genersCountDF = moviesDF.select(count(col("Major_Genre")))

//  genersCountDF.show()
  moviesDF.selectExpr("count(Major_Genre)")
  moviesDF.select(count(col("*")))//.show()

  moviesDF.select(countDistinct("Major_Genre"))//.show()

  moviesDF.select(min(col("US_Gross")))//.show()

  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))//.show()

  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )//.show()

  moviesDF
    .groupBy(col("Major_Genre")).count()//.show()

  moviesDF.groupBy(
    col("Major_Genre")
  ).avg("IMDB_Rating")//.show()

  moviesDF.groupBy(
    col("Major_Genre")
  ).agg(
    count("*").as("N_Movies"),
    avg("IMDB_Rating").as("Most Favourite Movies")
  ).show()

}
