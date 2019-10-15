
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

import geotrellis.layer.{LayoutDefinition, SpatialKey, ZoomedLayoutScheme}
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
import geotrellis.store.hadoop.util.HdfsUtils
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext

import org.log4s._

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

  def fetchRow(key: SpatialKey): Option[MbTiles.Row] = mbTiles.fetch(12, key.col, key.row)

  def allKeys: Iterator[SpatialKey] = mbTiles.allKeys(12)

  def allTiles: Iterator[MbTiles.Row] = mbTiles.allTiles(12)

  /** Query for all tiles that intersect LatLng extent */
  def queryForLatLngExtent(extent: Extent): Seq[MbTiles.Row] = {
    val wmExtent = ReprojectRasterExtent(RasterExtent(extent, 256, 256), LatLng, WebMercator).extent
    val tileBounds = layout.mapTransform.extentToBounds(wmExtent)
    mbTiles.fetchBounds(12, tileBounds)
  }
}

object OsmQaTiles {

  @transient private[this] lazy val logger = getLogger

  final val BASE_URL: String = "https://s3.amazonaws.com/mapbox/osm-qa-tiles-production/latest.country/"

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

  private val activeDownloads: TrieMap[Country, OsmQaTiles] = TrieMap.empty

  def fetchFor(country: Country): OsmQaTiles = activeDownloads.synchronized {
    activeDownloads.getOrElseUpdate(country, {
      //val sourceUri = OsmQaTiles.uriFromCode(country.code)
      val source = new Path(s"/${country.code}.mbtiles.gz") // default is HDFS root
      val localMbTiles = new File(s"/tmp/qatiles/${country.code}.mbtiles")

      if (!localMbTiles.exists) {
        logger.info(s"Copying: $source to $localMbTiles")
        HdfsUtils.read(source, new Configuration()) { is =>
          val bis = new BufferedInputStream(is)
          // HdfsUtils.read auto-detects and decompresses the GZip for us
          FileUtils.copyInputStreamToFile(bis, localMbTiles)
        }
        logger.info(s"Copying: $source to $localMbTiles (done)")
      } else {
        logger.info(s"Found: $localMbTiles")
      }

      new OsmQaTiles(localMbTiles)
    })
  }
}