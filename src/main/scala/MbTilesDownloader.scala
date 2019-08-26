package geotrellis.sdg

import org.apache.commons.io.IOUtils

import scalaj.http._

import java.io._
import java.net.URL
import java.util.zip.GZIPInputStream

import scala.util.Try


object MbTilesDownloader {
  final val BASE_URL: String = "https://s3.amazonaws.com/mapbox/osm-qa-tiles-production/latest.country/"

  def download(countryName: String): Unit = {
    val completeURL: String = s"${BASE_URL}${countryName.toLowerCase.replace(" ", "_")}.mbtiles.gz"

    val request: HttpRequest = Http(completeURL)

    val content: Array[Byte] = request.asBytes.body

    val zipStream = new GZIPInputStream(new ByteArrayInputStream(content))
    val byteStream = new ByteArrayOutputStream()

    val result = {
      IOUtils.copy(zipStream, byteStream)
      byteStream.toByteArray
    }

    zipStream.close
    byteStream.close

    val outputStream = new BufferedOutputStream(new FileOutputStream(s"/tmp/${countryName}.mbtiles"))

    Stream.continually(outputStream.write(result))

    outputStream.close()
  }
}
