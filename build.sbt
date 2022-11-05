name := "spark-essentials"

version := "0.1"

scalaVersion := "2.12.10"

val sparkVersion = "2.4.8"
val vegasVersion = "0.3.11"
val postgresVersion = "42.5.0"

resolvers ++= Seq(
  "bintray-spark-packages" at "https://dl.bintray.com/spark-packages/maven",
  "Typesafe Simple Repository" at "https://repo.typesafe.com/typesafe/simple/maven-releases",
  "MavenRepository" at "https://mvnrepository.com"
)


libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // logging
  "org.apache.logging.log4j" % "log4j-api" % "2.19.0",
  "org.apache.logging.log4j" % "log4j-core" % "2.19.0",
  // postgres for DB connectivity
  "org.postgresql" % "postgresql" % "42.5.0",

  // Mongo DB connectivity
  "org.mongodb.spark" %% "mongo-spark-connector" % "3.0.2",
)