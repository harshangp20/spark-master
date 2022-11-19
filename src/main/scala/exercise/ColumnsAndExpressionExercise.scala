package exercise

import exercise.ColumnsAndExpressionExercise.totalGrossOfMovie
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ColumnsAndExpressionExercise extends App {

  val spark = SparkSession
    .builder()
    .appName("Exercise Of Columns and Expression.")
    .config("spark.master", "local")
    .getOrCreate()
  val moviesDF = spark
    .read
    .option("inferSchema", "true")
    .option("multiline", "true")
    .json("src/main/resources/data/movies.json")

//  val myFavMovie = moviesDF.select("Title", "US_Gross").show()

  val totalGrossOfMovie =
    moviesDF.select(
      col("Title"),
      col("US_Gross"),
      col("Worldwide_Gross"),
      (col("US_Gross")
        + col("Worldwide_Gross"))
//        + col("US_DVD_Sales"))
        .as("Total Profit")
    )

  totalGrossOfMovie.show()

  val imdbRating = moviesDF.select("Title","IMDB_Rating")
    .where(
      col("Major_Genre") === "Comedy" and
        col("IMDB_Rating") > 6)

  imdbRating.show()

}
