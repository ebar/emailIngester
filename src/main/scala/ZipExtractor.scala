import java.io.{ByteArrayOutputStream, ByteArrayInputStream}
import java.util.zip.ZipInputStream


import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.typesafe.config.ConfigFactory
import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}
import com.amazonaws.services.s3.model.ObjectMetadata

object ZipExtractor extends App {

  val s3Client = AmazonS3ClientBuilder.standard()
    .withCredentials(new DefaultAWSCredentialsProviderChain)
    .withRegion(Regions.US_WEST_2)
    .build()

  val ingesterConfig = IngesterConfig(ConfigFactory.load())

  val conf = new SparkConf()
    .setAppName("zip-extractor")
    .setMaster(ingesterConfig.sparkMaster)
  val sc = new SparkContext(conf)

  val binaryFile = sc.binaryFiles(ingesterConfig.zipFilesDirectory)

  val filtered = binaryFile.filter(_._1.endsWith("_xml.zip"))

  val lines = filtered.foreach{ case (name, zipContent) => extract(zipContent)}

  def extract(zipContent: PortableDataStream): Unit = {
    val zis = new ZipInputStream(zipContent.open())
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (file.getName.endsWith(".txt") || file.getName.endsWith(".xml")) {
        if (!file.isDirectory) {
          val out = new ByteArrayOutputStream()
          val buffer = new Array[Byte](4096)
          Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
          val is = new ByteArrayInputStream(out.toByteArray)
          val meta = new ObjectMetadata()
          meta.setContentLength(out.size())
          s3Client.putObject(ingesterConfig.targetS3Bucket, file.getName, is, meta)
          out.close()
        }
      }
    }
  }

}
