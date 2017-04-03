import com.typesafe.config.Config

case class IngesterConfig(unzippedDirectory: String, unzippedS3Bucket: String, zipFilesDirectory: String, sparkMaster: String)

object IngesterConfig {

  def apply(config: Config) : IngesterConfig = {
    IngesterConfig(
      config.getString("enron.unzipped.directory"),
      config.getString("enron.unzipped.s3.bucket"),
      config.getString("enron.zipFiles.directory"),
      config.getString("spark.master")
    )
  }

}
