import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, Dataset, SparkSession}
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

object EnronEmailAnalyser extends App {

  val mode = Try(args(0)) match {
    case Success(m) => m
    case Failure(e) => throw new RuntimeException("Please specify 'local' or 'remote' as a command line argument")
  }

  val ingesterConfig = IngesterConfig(ConfigFactory.load(mode))

  val sparkSession = SparkSession
    .builder()
    .appName("email-analyser")
    .master(ingesterConfig.sparkMaster)
    .getOrCreate()

  new EnronEmailAnalyser(sparkSession, ingesterConfig).process()
}

class EnronEmailAnalyser(session: SparkSession, config: IngesterConfig) {

  def process() = {
    // Calculate word count from text files in the the text_000 folder

    val wordCount = calculateWordCount(s"${config.unzippedDirectory}/text_000")

    // Calculate top 100 recipients from xml files

    val res1: Dataset[Row] = calculateTopRecipients(s"${config.unzippedDirectory}/*.xml")

    //Display
    res1.show(100)
    println(s"Average email word count: $wordCount")
  }


  def calculateTopRecipients(xmlFileLocation: String): Dataset[Row] = {
    val xmlFiles = session
      .read.format("com.databricks.spark.xml")
      .option("rowTag", "Tags")
      .load(xmlFileLocation)

    val xmlExplodedDf = xmlFiles
      .withColumn("tags_exploded", explode(col("Tag")))

    val dfWithTags = xmlExplodedDf
      .withColumn("tag_name", tagName(col("tags_exploded")))
      .withColumn("tag_value", tagValue(col("tags_exploded")))

    val dfRecipientsWithWeighting = dfWithTags
      .filter(col("tag_name").isin("#To", "#CC"))
      .withColumn("weighting", calculateWeighting(col("tag_name")))
      .select(col("weighting"), explode(split(col("tag_value"), ", ")).alias("recipient"))

    val dfWeightedAndSummed = dfRecipientsWithWeighting
      .groupBy(col("weighting"), col("recipient"))
      .agg(sum(col("weighting")).alias("total_weighted"))

    dfWeightedAndSummed.groupBy("recipient")
      .agg(sum(col("total_weighted")).alias("total"))
      .orderBy(col("total").desc)
  }

  def calculateWordCount(directory: String) = {
    val emailFiles = session.sparkContext.wholeTextFiles(directory)
    emailFiles.map{ case (file, content) =>
      content.split(" ").length }.mean
  }

  def tagName = udf{(row: GenericRowWithSchema) => s"${row.getAs[String]("_TagName")}"}
  def tagValue = udf{(row: GenericRowWithSchema) => s"${row.getAs[String]("_TagValue")}"}
  def convertCSVIntoCount = udf{(list: String) => list.split(",").size}
  def calculateWeighting = udf{(tagName: String) => if (tagName.equals("#To")) 1.0 else 0.5}

}