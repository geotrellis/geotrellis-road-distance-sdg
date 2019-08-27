package geotrellis.sdg

import org.apache.commons.io.{IOUtils, FileUtils}

import scalaj.http._

import com.typesafe.scalalogging.LazyLogging

import java.io._
import java.net.URL
import java.util.zip.GZIPInputStream

import scala.util.Try


object MbTilesDownloader extends LazyLogging {
  final val BASE_URL: String = "https://s3.amazonaws.com/mapbox/osm-qa-tiles-production/latest.country/"

  def printURL(countryName: String): Unit =
    println(s"\n\n${BASE_URL}${countryName.toLowerCase.replace(" ", "_")}.mbtiles.gz\n")

  def getRequest(countryName: String): HttpRequest =
    Http(s"${BASE_URL}${countryName.toLowerCase.replace(" ", "_")}.mbtiles.gz")

  def download(countryName: String): Unit = {
    val completeURL: String = s"${BASE_URL}${countryName.toLowerCase.replace(" ", "_")}.mbtiles.gz"

    logger.info(s"\n\nDownloading this country: $countryName\n")

    val request: HttpRequest = Http(completeURL)

    request.execute[Unit] { inputStream =>
      val zipStream = new GZIPInputStream(inputStream)
      val targetFile = new File(s"/tmp/${countryName}.mbtiles")

      FileUtils.copyInputStreamToFile(zipStream, targetFile)

      zipStream.close()
      inputStream.close()
    }
  }
}
