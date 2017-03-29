import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions.udf

import scala.collection.mutable

object EnronEmailAnalyser extends App {

  val conf = new SparkConf().setAppName("email-analyser").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = SparkSession.builder().config(conf).appName("email-analyser").getOrCreate()

  // Read email file content from the text_000 directory

  val emailFiles = sc.wholeTextFiles("unzipped/text_000")

//  val wordCount = emailFiles.map{ case (file, content) =>
//    content.split(" ").length
//  }.mean
//
//  println(s"Average word count: $wordCount")

  // The XML file in each directory contains

  val xmlFiles = sqlContext.read.format("com.databricks.spark.xml")
    .option("rowTag", "Tags")
    .load("unzipped/*.xml")

  //xmlFiles.withColumn("NumberOfRecipients", countRecipients(xmlFiles("Tags")))


  val recipients = xmlFiles.select("Tags").explode(xmlFiles("Tags"))


  recipients.show


 // def countRecipients = udf{(tags: tagsSchema) =>  tags}


}