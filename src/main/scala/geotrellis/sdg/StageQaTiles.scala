package geotrellis.sdg

import com.monovore.decline.{CommandApp, Opts}
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.sql._
import org.locationtech.geomesa.spark.jts._

import software.amazon.awssdk.services.s3.model.GetObjectRequest

import java.io._

/**
 * Copy QA tiles from S3 to HDFS to avoid reading them
 */
object StageQaTiles extends CommandApp(
  name = "Stage MapBox QA tiles on HDFS",
  header = "copy from MapBox S3 bucket to cluster HDFS",
  main = {
    val countryCodesOpt = Opts.options[Country](long = "country", short = "c",
      help = "Country code to use for input").
      withDefault(Country.all)

    countryCodesOpt.map { countries =>

    System.setSecurityManager(null)
    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("CopyQATiles")

    implicit val spark: SparkSession =
      SparkSession.builder.config(conf).enableHiveSupport.getOrCreate.withJTS

    spark.sparkContext.
      parallelize(countries.toList, countries.length).
        foreach { country =>
          val (is, os): (InputStream, OutputStream) = country match {
            case cl: CountryLookup =>
              val path = new Path(s"hdfs:///${cl.code}.mbtiles.gz")
              val url = new AmazonS3URI(cl.mapboxQaTilesUrl)
              println(s"Copying $country: ${cl.mapboxQaTilesUrl} to $path")
              val fs = path.getFileSystem(new Configuration())
              val os = fs.create(path)
              val obj = GetObjectRequest.builder().bucket(url.getBucket).key(url.getKey).build()
              val is = S3ClientProducer.get().getObject(obj)
              (is, os)
            case _ =>
              println("QA Tiles are only used when results are computed from scratch; provided imagery should have already accounted for road networks")
              sys.exit()
          }

        try {
          IOUtils.copy(is, os)
          os.flush()
        } finally {
          is.close()
          os.close()
        }
      }

    spark.stop
  }
})