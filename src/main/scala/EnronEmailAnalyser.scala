import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

object EnronEmailAnalyser extends App {

  val conf = new SparkConf().setAppName("email-analyser").setMaster("local")
  val sc = new SparkContext(conf)

  val sqlContext = SparkSession
    .builder()
    .config(conf)
    .appName("email-analyser")
    .getOrCreate()

  // Read email file content from the text_000 directory. The mail content is found within the .eml files.
  // Assumption is to count the word length of the entire files which excludes attachments but does include some metadata information.

  val emailFiles = sc.wholeTextFiles("unzipped/text_000")
  val wordCount = emailFiles.map{ case (file, content) =>
  content.split(" ").length }.mean
  println(s"Average email word count: $wordCount")

  // The XML file in each directory contains a list of tags for each email.
  // The recipient information is contained within the two tag fields #To and #CC.

  val xmlFiles = sqlContext
    .read.format("com.databricks.spark.xml")
    .option("rowTag", "Tags")
    .load("unzipped/*.xml")

  val xmlExplodedDf = xmlFiles
    .withColumn("tags_exploded", explode(col("Tag")))

  val dfWithTags = xmlExplodedDf
    .withColumn("tag_name", tagName(col("tags_exploded")))
    .withColumn("tag_value", tagValue(col("tags_exploded")))

  val dfRecipientsWithWeighting = dfWithTags
    .filter(col("tag_name").isin("#To", "#CC"))
    .withColumn("weighting", calculateWeighting(col("tag_name")))
    .select(col("weighting"), explode(split(col("tag_value"), ",")).alias("recipient"))


  val dfWeightedAndSummed = dfRecipientsWithWeighting
    .groupBy(col("weighting"), col("recipient"))
    .agg(sum(col("weighting")).alias("total_weighted"))


  val res1 = dfWeightedAndSummed.groupBy("recipient")
    .agg(sum(col("total_weighted")).alias("total"))
    .orderBy(col("total").desc)

  println(s"Top 100 email recipients:")
  res1.show(100)


  def tagName = udf{(row: GenericRowWithSchema) => s"${row.getAs[String]("_TagName")}"}
  def tagValue = udf{(row: GenericRowWithSchema) => s"${row.getAs[String]("_TagValue")}"}
  def convertCSVIntoCount = udf{ (list: String) => list.split(",").size}
  def calculateWeighting = udf{ (tagName: String) => if (tagName.equals("#To")) 1.0 else 0.5}

}