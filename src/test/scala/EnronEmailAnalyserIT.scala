
import org.apache.spark.sql.{Row, SparkSession}
import org.scalatest.{Matchers, FlatSpec}

class EnronEmailAnalyserIT extends FlatSpec with Matchers {

  val testConfig = IngesterConfig("", "", "", "")

  it should "calculate average word count of email files" in {

    val sparkSession = SparkSession
      .builder()
      .appName("test-email-analyser")
      .master("local")
      .getOrCreate()

    val emailAnalyser = new EnronEmailAnalyser(sparkSession, testConfig)
    val wordCount = emailAnalyser.calculateWordCount("src/test/resources/test_email_dir")
    wordCount should be (124.0)
  }

  it should "calculate top recipients" in {
    val sparkSession = SparkSession
      .builder()
      .appName("test-email-analyser")
      .master("local")
      .getOrCreate()

    val emailAnalyser = new EnronEmailAnalyser(sparkSession, testConfig)
    val topRecipients = emailAnalyser.calculateTopRecipients("src/test/resources/test_xml.xml")

    val result = topRecipients.take(5)
    result(0) should be(Row("pete.davis@enron.com", 4.5))
    result(1) should be(Row("monika.causholli@enron.com", 3.5))
    result(2) should be(Row("bill.williams.III@enron.com" ,2.5))
    result(3) should be(Row("Eric.Linder@enron.com", 2.0))
    result(4) should be(Row("ryan.slinger@enron.com", 1.5))
  }

}
