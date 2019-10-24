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
import org.apache.spark.sql.functions.{col, expr, udf}
import org.locationtech.jts.geom.TopologyException
import org.locationtech.jts.operation.union.CascadedPolygonUnion
import org.log4s._
import spire.syntax.cfor._
import vectorpipe.VectorPipe

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.net.URI

/**
 * for running operations on a pre-calculated tif
 */
class PopulationNearRoadsProvidedJob(
  countryProvided: CountryProvided,
  layout: LayoutDefinition,
  crs: CRS
)(implicit spark: SparkSession) extends PopulationNearRoadsJob with Serializable {

  @transient private[this] lazy val logger = getLogger

  @transient lazy val rasterSource = countryProvided.rasterSource.reprojectToGrid(crs, layout)

  @transient lazy val layoutTileSource: LayoutTileSource[SpatialKey] =
    LayoutTileSource.spatial(rasterSource, layout)

  val wsCountryBorder: MultiPolygon =
    countryProvided.boundary.reproject(rasterSource.crs, layoutTileSource.source.crs)

  val rsRdd: RDD[RasterSource] = spark.sparkContext.parallelize(Array(rasterSource), 1)

  // Generate per-country COG regions we will need to read.
  // These do not contain duplicate reads of pixels - each key maps to COG segment
  val regionsRdd: RDD[(SpatialKey, Unit)] = {
    val regions = layoutTileSource.layout.mapTransform.keysForGeometry(wsCountryBorder).map({ key => (key, ())})
    spark.sparkContext
      .parallelize(regions.toSeq, 1)
      .setName(s"Provided RasterSource Regions")
      .cache()
  }

  val partitioner: Partitioner = {
    val regionCount = regionsRdd.count
    val partitions = math.max(1, regionCount / 16).toInt
    logger.info(s"Partitioner: ProvidedImage using $partitions partitions for $regionCount regions")
    //SpatialPartitioner(partitions, bits = 4)
    new HashPartitioner(partitions)
  }

  def popRegions: RDD[(SpatialKey, SummaryRegion)] =
    regionsRdd.flatMap { case (key, _) =>
        // !!! This part is CRITICAL !!!
        // We moved the key and not the RasterRegion because each regions will spawn a new RasterSource
        // This would result in new S3Client and Metadata fetch for EVERY segment read.
        // So we generate RasterRegion from key inside the map step
        layoutTileSource.rasterRegionForKey(key).map({ region =>
          (key, SummaryRegion(region, None, None))
        })
      }

  val forgottenLayer: TileLayerRDD[SpatialKey] = {
    val rdd = popRegions.flatMap { case (key, region) =>
      region.forgottenPopTile.map(tile => (key, tile))
    }

    // This is the first time in the job flow we're trying to read COG on driver, log for tracing
    logger.info(s"Reading COG: ${rasterSource.name}")
    val md = TileLayerMetadata(rasterSource.cellType, layout, rasterSource.extent,
      rasterSource.crs, KeyBounds(layout.mapTransform.extentToBounds(rasterSource.extent)))

    ContextRDD(rdd, md)
  }

  def persist: Unit = ()

  def unpersist: Unit = ()

  val result: (PopulationSummary, StreamingHistogram) =
    popRegions.flatMap({ case (_, r) =>
      for {
        tile: Tile <- r.forgottenPopTile
        sum <- r.summary
      } yield {
        val hist = StreamingHistogram(256)
        tile.foreachDouble(c => hist.countItem(c, 1))
        (sum, hist)
      }
    }).reduce({ case ((s1, h1), (s2, h2)) => (s1.combine(s2), h1.merge(h2))})
}
