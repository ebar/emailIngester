import java.io.{ByteArrayOutputStream, FileOutputStream, InputStreamReader, BufferedReader}
import java.nio.file.{FileSystems, Path}
import java.util.zip.ZipInputStream

import org.apache.spark.input.PortableDataStream
import org.apache.spark.{SparkConf, SparkContext}

object ZipExtractor extends App {

  val conf = new SparkConf().setAppName("zip-extractor").setMaster("local")
  val sc = new SparkContext(conf)

  val destination = FileSystems.getDefault.getPath("unzipped")

  val binaryFile = sc.binaryFiles("edrm-enron-v2_linder-e_xml.zip")

  val filtered = binaryFile.filter(_._1.endsWith("_xml.zip"))

  val lines = filtered.foreach{ case (name, zipContent) => extract(zipContent)}

  def extract(zipContent: PortableDataStream): Unit = {
    val zis = new ZipInputStream(zipContent.open())
    Stream.continually(zis.getNextEntry).takeWhile(_ != null).foreach { file =>
      if (file.getName.endsWith(".txt") || file.getName.endsWith(".xml")) {
        if (!file.isDirectory) {
          val outPath = destination.resolve(file.getName)
          val outPathParent = outPath.getParent
          if (!outPathParent.toFile.exists()) {
            outPathParent.toFile.mkdirs()
          }

          val outFile = outPath.toFile
          val out = new FileOutputStream(outFile)
          val buffer = new Array[Byte](4096)
          Stream.continually(zis.read(buffer)).takeWhile(_ != -1).foreach(out.write(buffer, 0, _))
          out.close()
        }
      }
    }
  }

}
