
/*
 * Copyright 2018 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package geotrellis.qatiles

import com.typesafe.scalalogging.LazyLogging
import geotrellis.layer.{LayoutDefinition, ZoomedLayoutScheme}
import geotrellis.vectortile.VectorTile
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.sdg._
import java.io._
import java.net.URI
import java.net.URL
import java.util.zip.GZIPInputStream

import geotrellis.raster.RasterExtent
import geotrellis.raster.reproject.ReprojectRasterExtent
import org.apache.commons.io.{FileUtils, IOUtils}

import scala.util.Try
import scalaj.http._

import scala.collection.concurrent.TrieMap
import scala.concurrent._


/**
 * Local, unzipped copy of QA Tiles database for a country that can be queried for a tile.
 */
class OsmQaTiles(
  val localMbTile: File
) extends Serializable {
  val scheme: ZoomedLayoutScheme = ZoomedLayoutScheme(WebMercator, 256)
  val layout: LayoutDefinition = scheme.levelForZoom(12).layout
  val mbTiles: MbTiles = new MbTiles(localMbTile, scheme)

  /** Query for all tiles that intersect LatLng extent */
  def queryForLatLngExtent(extent: Extent): Seq[VectorTile] = {
    val wmExtent = ReprojectRasterExtent(RasterExtent(extent, 256, 256), LatLng, WebMercator).extent
    val tileBounds = layout.mapTransform.extentToBounds(wmExtent)
    mbTiles.fetchBounds(12, tileBounds)
  }
}

object OsmQaTiles extends LazyLogging {
  final val BASE_URL: String = "https://s3.amazonaws.com/mapbox/osm-qa-tiles-production/latest.country/"

  def uriFromName(countryName: String): URI =
    new URI(s"${BASE_URL}${countryName.toLowerCase.replace(" ", "_")}.mbtiles.gz")

  /** Downloads, throws  */
  private def download(source: URI, target: File): Unit = {
    logger.info(s"Downloading $source to $target")
    val request: HttpRequest = Http(source.toString)

    request.execute[Unit] { inputStream =>
      val zipStream = new GZIPInputStream(inputStream)
      FileUtils.copyInputStreamToFile(zipStream, target)
      zipStream.close()
    }
  }

  private val activeDownloads: TrieMap[Country, Future[OsmQaTiles]] = TrieMap.empty
  def fetchFor(country: Country): Future[OsmQaTiles] = activeDownloads.synchronized {
    activeDownloads.getOrElseUpdate(country,
      Future {
        val sourceUri = OsmQaTiles.uriFromName(country.name)
        val localMbTiles = new File(s"/tmp/qatiles/${country.code}.mbtiles")
        if (!localMbTiles.exists) OsmQaTiles.download(sourceUri, localMbTiles)
        new OsmQaTiles(localMbTiles)
      }(scala.concurrent.ExecutionContext.Implicits.global))
  }
}