name := "emailIngester"

version := "1.0"

scalaVersion := "2.11.8"


libraryDependencies ++= Seq("org.apache.spark" % "spark-core_2.11" % "2.0.1",
                            "org.apache.spark" % "spark-sql_2.11" % "2.0.1",
                            "com.amazonaws" % "aws-java-sdk-s3" % "1.11.112",
                            "com.databricks" % "spark-xml_2.11" % "0.4.1",
                            "com.typesafe" % "config" % "1.3.1"
)