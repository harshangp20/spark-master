package dataSources

import org.apache.spark.sql.SparkSession

//noinspection DuplicatedCode
object DataSources extends App {

  val path = "data/stocks.csv"
  val driver = "org.postgresql.Driver"
  val connector = "jdbc:postgresql://localhost:5432/rtjvm"
  val table = "public.movies"
  val userName = "docker"
  val password = "docker"

  val spark = SparkSession
    .builder()
    .appName("Data Source Learnings")
    .config("spark.master", "local")
    .getOrCreate()

  //  val carsSchema = StructType(Array(
  //    StructField("Name", StringType),
  //    StructField("Miles_per_Gallon", DoubleType),
  //    StructField("Cylinders", LongType),
  //    StructField("Displacement", DoubleType),
  //    StructField("Horsepower", LongType),
  //    StructField("Weight_in_lbs", LongType),
  //    StructField("Acceleration", DoubleType),
  //    StructField("Year", DateType),
  //    StructField("Origin", StringType)
  //  ))

  //  val carsDFWithSchema = spark.read
  //    .format("json")
  //    .schema(carsSchema) // enforce a schema
  //    .option("mode", "failFast") //dropMalformed, permissive (default)
  //    .option("path", "data/cars.json")
  //    .load()
  //  carsDFWithSchema.show()
  //  //
  //  carsDFWithSchema.show()
  //    val carsDFWithMap = spark.read
  //      .format("json")
  //      .options(Map(
  //        "mode" -> "failFast",
  //        "path" -> "data/cars.json"
  //      ))
  //      .load()
  //
  //    carsDFWithMap.show()
  //
  //  // overwrite particular file to the path
  //  carsDFWithMap.write
  //    .format("json")
  //    .mode(SaveMode.Overwrite)
  //    .save("data/cars_duplicate.json")

  //  spark.read
  //    .schema(carsSchema)
  //    .option("dateFormat","DD/MM/YY")
  //    .option("allowSingleQuotes","true")
  //    .option("compression","uncompressed")
  //    .json("data/cars.json")
  //    .show()

  //  reading CSV file

  //  val stocksSchema = StructType(Array(
  //    StructField("symbol", StringType),
  //    StructField("date", DateType),
  //    StructField("price", DoubleType)
  //  ))
  //
  //  val csvData = spark.read
  //    .format("csv")
  //    .option("inferSchema", "true")
  //    .schema(stocksSchema)
  //    .option("header", "true")
  //    .option("dateFormat", "MMM dd YYYY")
  //    .option("sep", ",")
  //    .option("nullValue", "")
  //    .load(path)
  //
  //      csvData.toDF()
  //        .withColumn("date", to_date($"date","MMM dd YYYY"))
  //        .show()
  //
  //      csvData.printSchema()
  //      csvData.show()
  //
  //  //  Parquet
  //    carsDFWithMap.write
  //      .mode(SaveMode.Overwrite)
  //      .save("data/cars.parquet"
  //      spark.read.text("data/aboutScala.txt").show()
  //
  //    val emoloyeesDF = spark.read
  //      .format("jdbc")
  //      .option("driver", "org.postgresql.Driver")
  //      .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
  //      .option("user", "docker")
  //      .option("password", "docker")
  //      .option("dbtable", "public.employees")
  //      .load()
  //
  //      emoloyeesDF.show()

//  val moviesDF = spark.read.json("data/movies.json")
//  moviesDF.write
//    .format("csv")
//    .option("header", "true")
//    .option("seq", "\t")
//    .save("src/main/resources/data/movies.csv")
//
//  val moviesCSV = spark.read.csv("src/main/resources/data/movies.csv/part-00000-d1508fd2-821c-4708-ae08-efae87128500-c000.csv")
//  moviesCSV.show()
//
//  moviesDF.write.save("src/main/resources/data/movies.parquet")


  //  moviesDF.write
  //    .format("jdbc")
  //    .option("driver", driver)
  //    .option("url", connector)
  //    .option("user", userName)
  //    .option("password", password)
  //    .option("dbtable", "movies")
  //    .save()

}
