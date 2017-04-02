
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{Matchers, FlatSpec}

class EnronEmailAnalyserITTest extends FlatSpec with Matchers {

  it should "calculate average word count of email files" in {

    val sparkSession = SparkSession
      .builder()
      .appName("test-email-analyser")
      .master("local")
      .getOrCreate()

    val emailAnalyser = new EnronEmailAnalyser(sparkSession)
    val wordCount = emailAnalyser.calculateWordCount("src/test/resources/test_email_dir")
    wordCount should be (124.0)
  }

  it should "calculate top recipients" in {
    val sparkSession = SparkSession
      .builder()
      .appName("test-email-analyser")
      .master("local")
      .getOrCreate()

    val emailAnalyser = new EnronEmailAnalyser(sparkSession)
    val topRecipients = emailAnalyser.calculateTopRecipients("src/test/resources/test_xml.xml")


    val topRecipient = topRecipients.head()
    topRecipient should be(Row("pete.davis@enron.com", 4.5)) //

  }

}
