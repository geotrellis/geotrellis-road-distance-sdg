package geotrellis.sdg

import geotrellis.layer._
import geotrellis.proj4._
import geotrellis.qatiles.{OsmQaTiles, RoadTags}
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster._
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffBuilder, GeoTiffOptions, SinglebandGeoTiff, Tags, Tiled}
import geotrellis.spark.{ContextRDD, TileLayerRDD}
import geotrellis.vector._
import geotrellis.vectortile.VectorTile
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.locationtech.jts.geom.TopologyException
import org.locationtech.jts.operation.union.CascadedPolygonUnion
import org.log4s._
import spire.syntax.cfor._
import vectorpipe.VectorPipe

import com.uber.h3core.H3Core

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.net.URI

trait PopulationNearRoadsJob {
  def result: (PopulationSummary, StreamingHistogram)
  // layer with counts of people that don't live within 2km of all-weather roads
  def forgottenLayer: TileLayerRDD[SpatialKey]
  // RDDs that will need recomputation can registered via persist
  def persist(): Unit
  // RDDs that will need recomputation can be deregistered via unpersist
  def unpersist(): Unit
}

object PopulationNearRoadsJob {

  @transient private[this] lazy val logger = getLogger

  def forgottenLayerTiles(
    forgottenLayer: TileLayerRDD[SpatialKey],
    outputUri: URI,
    layout: LayoutDefinition
  )(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val maxZoom = 10
    val minZoom = 6

    val gridPointsRdd: RDD[(String, Double)] = forgottenLayer.flatMap {
      case (key: SpatialKey, tile: Tile) => {
        val h3: H3Core = H3Core.newInstance
        val tileExtent = key.extent(layout)
        val re = RasterExtent(tileExtent, tile)
        for {
          col <- Iterator.range(0, tile.cols)
          row <- Iterator.range(0, tile.rows)
          v = tile.getDouble(col, row)
          if isData(v)
        } yield {
          val (lon, lat) = re.gridToMap(col, row)
          // Higher number makes larger hexagons
          // Zero means that our starting maxZoom == h3 hex "resolution"
          val hexZoomOffset = 2
          val h3Index = h3.geoToH3Address(lat, lon, maxZoom - hexZoomOffset)
          (h3Index, v)
        }
      }
    }

    val pipeline = ForgottenPopPipeline(
      "geom",
      outputUri,
      maxZoom
    )
    val vpOptions = VectorPipe.Options(
      maxZoom = maxZoom,
      minZoom = Some(minZoom),
      srcCRS = WebMercator,
      destCRS = None,
      useCaching = false,
      orderAreas = false
    )

    val gridPointsDf = gridPointsRdd
      .toDF("h3Index", "pop")
      .withColumn("geom", pipeline.geomUdf(col("h3Index")))
    VectorPipe(gridPointsDf, pipeline, vpOptions)
  }

  /** Save country as GeoTIFF for inspection
   * @note This method relies on collecting layer tiles to the master.
   *       Don't expect it to work for larger countries.
   */
  def layerToGeoTiff(layer: TileLayerRDD[SpatialKey]): SinglebandGeoTiff = {
    val builder = GeoTiffBuilder.singlebandGeoTiffBuilder
    val md = layer.metadata
    val segments = layer.collect().toMap
    val tile = builder.makeTile(segments.toIterator, md.layout.tileLayout, md.cellType, Tiled(256), DeflateCompression)
    val extent = md.layout.extent
    builder.makeGeoTiff(tile, extent, md.crs, Tags.empty, GeoTiffOptions.DEFAULT)
  }
}