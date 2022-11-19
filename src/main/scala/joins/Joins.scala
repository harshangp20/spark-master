package joins

import org.apache.spark.sql.SparkSession

object Joins extends App {

   val spark = SparkSession
     .builder()
     .appName("JOINS")
     .config("spark.master","local")
     .getOrCreate()

  val bandsDF = spark
    .read
    .option("inferSchema","true")
    .option("multiline","true")
    .json("src/main/resources/data/bands.json")

  val guitarsDF = spark
    .read
    .option("inferSchema","true")
    .option("multiline","true")
    .json("src/main/resources/data/guitars.json")

  val guitarPlayersDF = spark
    .read
    .option("inferSchema","true")
    .option("multiline","true")
    .json("src/main/resources/data/guitarPlayers.json")

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")

//  inner
  val guitarsArtistDF = guitarPlayersDF.join(bandsDF, joinCondition,"inner")
//  guitarsArtistDF.show

//  left_outer
  guitarPlayersDF.join(bandsDF, joinCondition,"left_outer")//.show()

//  right_outer
  guitarPlayersDF.join(bandsDF, joinCondition,"right_outer")//.show()

//  outer
  guitarPlayersDF.join(bandsDF, joinCondition,"outer")//.show()

//  left_semi
  guitarPlayersDF.join(bandsDF, joinCondition,"left_semi")//.show()

//  anti-joins
  guitarPlayersDF.join(bandsDF, joinCondition,"left_anti")//.show()

  guitarsArtistDF.select("id","band")//.show()

  //Rename 

}
