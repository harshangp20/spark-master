package joins

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object JoinExe extends App {

  val spark = SparkSession
    .builder()
    .appName("JOINS")
    .config("spark.master","local")
    .getOrCreate()

  val path = "data/stocks.csv"
  val driver = "org.postgresql.Driver"
  val connector = "jdbc:postgresql://localhost:5432/rtjvm"
  val table = "public.movies"
  val userName = "docker"
  val password = "docker"

  def readTable(tableName: String) = {
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", connector)
      .option("user", userName)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()
  }

  val employeeDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagerDF = readTable("dept_manager")
  val titleDF = readTable("titles")

  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(max("salary").as("to_date"))//.show()
  val employeesSalariesDF = employeeDF.join(maxSalariesPerEmpNoDF,"emp_no")
//  employeesSalariesDF.show()

  val empNeverManagersDF =
    employeeDF
      .join(deptManagerDF,
        employeeDF.col("emp_no") ===
          deptManagerDF.col("emp_no")
        ,"left_anti")

//  empNeverManagersDF.show()

  val mostRecentJobTitlesDF =
    titleDF.groupBy("emp_no").agg(max("to_date").as("to_date"))
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobsDF = bestPaidEmployeesDF.join(mostRecentJobTitlesDF,"emp_no")

  bestPaidJobsDF.show()

}
