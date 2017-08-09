name := "Sample Project
version := "1.0"
scalaVersion := "2.10.5"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-hive" $ "1.6.0",
  "org.apache.hadoop" % "hadoop-core" % "0.20.2",
  "org.apache.spark" % "spark-mllib_2.10" % "1.6.0"
)
