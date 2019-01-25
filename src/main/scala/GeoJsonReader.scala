package geotrellis.sdg

import java.io.FileNotFoundException
import java.net.URL
import java.security.InvalidParameterException

import com.typesafe.scalalogging.LazyLogging

import geotrellis.vector._
import geotrellis.vector.io._
import geotrellis.vector.io.json._
import geotrellis.spark.io.s3._

import spray.json._
import DefaultJsonProtocol._

import scala.collection.JavaConverters._

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.util.zip.ZipInputStream


object GeoJsonReader extends LazyLogging {
  // Greedy match, will trim white space around but won't ensure proper GeoJSON
  val FeatureRx = """.*(\{\"type\":\"Feature\".+}).*""".r

  def readFromS3(bucket: String, prefix: String): Vector[MultiPolygonFeature[Map[String, String]]] = {
    val client = S3Client.DEFAULT
    val bytes = client.readBytes(bucket, prefix)
    val jsonString = (bytes.map { _.toChar }).mkString

    val collection = GeoJson.parse[JsonFeatureCollection](jsonString)
    val features = collection.getAllFeatures[MultiPolygonFeature[Map[String, String]]]

    features.flatMap { feature =>
      if (feature.geom.isValid)
        Some(feature)
      else {
        logger.warn(s"Dropping invalid feature: ${feature.geom.toWKT}\n${feature.data}")
        None
      }
    }
  }

  /**
    * California.geojson is 2.66GB uncompressed, need to read it as a stream to avoid blowing the heap\
    * Supports: .zip, .json, .geojson files
  */
  def readFromGeoJson(url: URL): Iterator[MultiPolygonFeature[Map[String, String]]] = {
    // TODO: consider how bad it is to leave the InputStream open
    // TODO: consider using is.seek(n) to partition reading the list
    val is: InputStream = url.getPath match {
      case null =>
        throw new FileNotFoundException("Can't read")

      case p if p.endsWith(".geojson") || p.endsWith(".json") =>
        url.openStream()

      case p if p.endsWith(".zip") =>
        val zip = new ZipInputStream(url.openStream)
        val entry = zip.getNextEntry
        logger.info(s"Reading: $url - ${entry.getName}")
        zip

      case _ =>
        throw new InvalidParameterException(s"Can't read: $url format")
    }

    val reader: BufferedReader = new BufferedReader(new InputStreamReader(is))
    val stream = reader.lines()
    stream.iterator().asScala.flatMap {
      case FeatureRx(json) =>
        val collection = json.parseGeoJson[JsonFeatureCollection]
        val features = collection.getAllFeatures[MultiPolygonFeature[Map[String, String]]]

        features.flatMap { feature =>
          if (feature.geom.isValid)
            Some(feature)
          else {
            logger.warn(s"Dropping invalid feature: ${feature.geom.toWKT}\n${feature.data}")
            None
          }
        }
      case _ =>
        None
    }
  }
}
