package exercise

import dataframes.DataFramesBasics.spark
import org.apache.spark.sql.SparkSession

object DataFrameExercise extends App {

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .getOrCreate()

  val smartPhoneDetails = Seq (
    ("Redmi Note 4 pro","2018","fhd display 6.1\"", "12mp"),
    ("Redmi Note 5 pro","2019","Amoled display 6.5\"", "24mp"),
    ("Realme 6 pro","2020","Amoled display 6.3\"", "36mp"),
    ("Samsung A55","2020","Samoled display 6.4\"", "64mp"),
    ("Realme 9 pro 5G","2022","fhd display 6.5\"", "64mp"),
    ("Redmi Note 10 pro","2018","fhd display 6.1\""," 64mp"),
  )

  val smartPhone = spark.createDataFrame(smartPhoneDetails)

  import spark.implicits._

  val manualImplicitsOfMobile =
    smartPhoneDetails.toDF("Name","Model","Screen Dimension","Camera Megapixels")

//  manualImplicitsOfMobile.show()

  val readingFile = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("data/movies.json")

//  readingFile.show()
  readingFile.printSchema()

  println(s"The movies has ${readingFile.count()} rows")
//  readingFile.columns

}
