import com.typesafe.config.Config

case class IngesterConfig(targetS3Bucket: String, zipFilesDirectory: String, sparkMaster: String)

object IngesterConfig {

  def apply(config: Config) : IngesterConfig = {
    IngesterConfig(
      config.getString("enron.extracted.s3.bucket"),
      config.getString("enron.zipFiles.directory"),
      config.getString("spark.master")
    )
  }

}
