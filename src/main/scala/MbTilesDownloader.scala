package geotrellis.sdg

import scalaj.http.Http

import org.apache.commons.io.IOUtils

import java.io._
import java.net.URL
import java.util.zip._

import scala.util.Try


object MbTilesDownloader {
  final val BASE_URL: String = "https://s3.amazonaws.com/mapbox/osm-qa-tiles-production/latest.country/"

  def download(countryName: String): Unit = {
    val completeURL: String = s"${BASE_URL}${countryName.toLowerCase.replace(" ", "_")}.mbtiles.gz"

    val request = Http(completeURL)

    val totalLength: Long = {
      val headers = request.method("GET").execute { is => "" }

      headers
        .header("Content-Length")
        .flatMap ({ cl => Try(cl.toLong).toOption }) match {
          case Some(num) => num
          case None => throw new Exception(s"Could not read from url: $completeURL")
        }
    }

    val content: Array[Byte] =
      request
        .method("GET")
        .header("Range", s"bytes=0-${totalLength}")
        .asBytes
        .body

    val zipStream = new ZipInputStream(new ByteArrayInputStream(content))
    val byteStream = new ByteArrayOutputStream()

    val result = {
      IOUtils.copy(zipStream, byteStream)
      byteStream.toByteArray
    }

    println(s"\n\nThis is the size of the result: ${result.size}\n\n")

    zipStream.close
    byteStream.close

    val outputStream = new BufferedOutputStream(new FileOutputStream(s"/tmp/${countryName}.mbtiles"))

    Stream.continually(outputStream.write(result))

    outputStream.close()
  }
}
