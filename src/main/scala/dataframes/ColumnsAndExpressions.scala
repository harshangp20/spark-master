package dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession
    .builder()
    .appName("DF WITH COLUMNS AND EXPRESSIONS")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark
    .read
    .option("inferSchema", "true")
    .option("multiline", "true")
    .json("data/cars.json")

  val firstColumn = carsDF.col("Name")

  val carNameDF = carsDF.select(firstColumn)

  //  carNameDF.show()

  import spark.implicits._

  val carsDetails = carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    col("weight_in_lbs"),
    'Year,
    $"Horsepower",
    expr("Origin")
  )

  //  carsDetails.show()


  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  //    carsWithWeightsDF.show()

  val carsWithSelectWeightsDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in Pounds")

  carsWithColumnRenamed.selectExpr("'Weight in pounds'")

  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  //  Filter
    val europeanCarsDFWithFilter = carsDF.filter(col("Origin") =!= "USA")
    val europeanCarsDFExtra = carsDF.filter(col("Origin") =!= "USA" && col("Origin") =!= "Japan")

  //  Where
  val europeanCarsDFWithWhere = carsDF.where(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val europeanCarsDFWithWhere2 = carsDF.where((col("Origin") === "USA" and col("Horsepower") > 150)
    or (col("Origin") === "Japan" and col("Horsepower") < 100))

  val europeanCarsDFWithWhere3 = carsDF.filter("Origin = 'USA' and Horsepower > 150 " +
    "or Origin = 'Japan' and Horsepower < 100")

  val moreCars = spark
    .read
    .option("inferSchema","true")
    .json("data/guitars.json")
//    .cache()

  val moreCars2 = spark
    .read
    .option("inferSchema","true")
    .json("data/guitars1.json")
//    .cache()

  /*val a = moreCars.filter($"_corrupt_record".isNull).count()
  println(a)*/
  val moreAndMoreCars = moreCars.union(moreCars2)

  moreAndMoreCars
    .show()

//  europeanCarsDFWithFilter.show()
//  europeanCarsDFExtra.show()
//  europeanCarsDFWithWhere.show()
//  europeanCarsDFWithWhere2.show()
//  europeanCarsDFWithWhere3.show()
//  moreCars.show()
//  moreAndMoreCars.show()
}
