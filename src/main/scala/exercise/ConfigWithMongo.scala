package exercise

import dataframes.ColumnsAndExpressions.spark
import org.apache.spark.sql.SparkSession

object ConfigWithMongo extends App {

  import com.mongodb.spark._

  val sparkConnectorForMongoDB = SparkSession
    .builder()
    .appName("MongoSparkConnectorIntro")
    .config("spark.master","local")
    .config("spark.mongodb.input.uri","mongodb://localhost:27017/Flights")
    //    .config("spark.mongodb.output.uri","mongodb://127.0.0.1:27017/Flights/flightData")
    .getOrCreate()

  val df = sparkConnectorForMongoDB
    .read
    .option("inferSchema", "true")
    .format("mongo")
    .option("database", "Flights")
    .option ("collection","flightData")
    .load()

  import spark.implicits._
  val specificColumns = df.col("status")

  val statusDF = df.select(specificColumns)

//  df.filter("flightData.status")

  df.show()

//  statusDF.show()

//  MongoSpark
}
