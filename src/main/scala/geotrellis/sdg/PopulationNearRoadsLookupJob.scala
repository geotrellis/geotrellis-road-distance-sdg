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

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, udf}

import org.locationtech.jts.geom.TopologyException
import org.locationtech.jts.operation.union.CascadedPolygonUnion
import org.log4s._
import spire.syntax.cfor._
import vectorpipe.VectorPipe

import com.uber.h3core.H3Core

import scala.collection.JavaConverters._
import scala.collection.mutable
import java.net.URI

/**
  * Here we're going to start from country mbtiles and then do ranged reads
  */
class PopulationNearRoadsLookupJob(
  country: CountryLookup,
  grumpRdd: RDD[Geometry],
  layout: LayoutDefinition,
  crs: CRS,
  roadFilter: RoadTags => Boolean
)(implicit spark: SparkSession) extends PopulationNearRoadsJob with Serializable {

  @transient private[this] lazy val logger = getLogger

  @transient lazy val rasterSource = country.rasterSource.reprojectToGrid(crs, layout)

  @transient lazy val layoutTileSource: LayoutTileSource[SpatialKey] =
    LayoutTileSource.spatial(rasterSource, layout)

  val wsCountryBorder: MultiPolygon = country.boundary.reproject(LatLng, layoutTileSource.source.crs)
  val countryRdd: RDD[CountryLookup] = spark.sparkContext.parallelize(Array(country), 1)

  // Generate per-country COG regions we will need to read.
  // These do not contain duplicate reads of pixels - each key maps to COG segment
  val regionsRdd: RDD[(SpatialKey, Unit)] =
    countryRdd.flatMap({ country =>
      // WARN: who says these COGs exists there at all (USA does not) ?
      logger.info(s"Reading: $country ${layoutTileSource.source.name}")
      layoutTileSource.layout.mapTransform.keysForGeometry(wsCountryBorder).map { key => (key, ())}
    }).setName(s"${country.code} Regions").cache()

  val partitioner: Partitioner = {
    val regionCount = regionsRdd.count
    val partitions = math.max(1, regionCount / 16).toInt
    logger.info(s"Partitioner: ${country.code} using $partitions partitions for $regionCount regions")
    //SpatialPartitioner(partitions, bits = 4)
    new HashPartitioner(partitions)
  }

  // Each buffered road "overflowed", we need to join it back up, not going to trim it tough
  val roadMaskRdd: RDD[(SpatialKey, MutableArrayTile)] = {
    val allKeysRdd = countryRdd.flatMap { country =>
      val qaTiles = OsmQaTiles.fetchFor(country)
      qaTiles.allKeys.map { key => (key, Unit) }
    }.setName(s"${country.code} MBTile Keys").cache()

    allKeysRdd.partitionBy(partitioner).flatMap { case (key, _) =>
      val qaTiles = OsmQaTiles.fetchFor(country)
      val row = qaTiles.fetchRow(key).get
      val roads: Seq[MultiLineString] = PopulationNearRoadsLookupJob.extractRoads(row.tile, roadFilter)
      val buffered = roads.map(GeometryUtils.bufferByMeters(_, WebMercator, layoutTileSource.source.crs, meters = 2000))
      PopulationNearRoadsLookupJob.burnRoadMask(layoutTileSource.layout, buffered)
    }
  }.reduceByKey(partitioner, (l, r) => PopulationNearRoadsLookupJob.combineMasks(l, r))

  // We have RDD of countries and COG keys we will need to read.
  // However, there could be pop regions NOT covered by COGs that we still need to read
  // So we should consider all regions available form WorldPop and join them to vectors

  // TODO: try read and tile approach for performance
  val grumpMaskRdd: RDD[(SpatialKey, Tile)] =
    Grump.masksForBoundary(grumpRdd, layout, wsCountryBorder, partitioner)
      .setName(s"${country.code} GRUMP Mask")

  val popRegions: RDD[(SpatialKey, SummaryRegion)] =
    regionsRdd.
      cogroup(roadMaskRdd, grumpMaskRdd, partitioner).
      flatMap { case (key, (_, roadMasks, grumpMasks)) =>
        // !!! This part is CRITICAL !!!
        // We moved the key and not the RasterRegion because each regions will spawn a new RasterSource
        // This would result in new S3Client and Metadata fetch for EVERY segment read.
        // So we generate RasterRegion from key inside the map step
        layoutTileSource.rasterRegionForKey(key).
          map({ region =>
            val sm = SummaryRegion(region, roadMasks.headOption, grumpMasks.headOption)
            (key, sm)
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

  def forgottenLayerTiles(outputUri: URI, pixelScale: Int): Unit = {
    import spark.implicits._
    val maxZoom = 12
    val minZoom = 6
    // Tweak this value by powers of 2 to increase or reduce the pixel size in the output
    // Higher number == smaller pixels
    val pixelsPerTile = math.pow(2, pixelScale).toInt
    val wmLayoutScheme = ZoomedLayoutScheme(WebMercator, pixelsPerTile)
    val wmLayout: LayoutDefinition = wmLayoutScheme.levelForZoom(maxZoom).layout
    val latLngToWebMercator = Transform(LatLng, WebMercator)

    val gridPointsRdd: RDD[(Long, Long, Double)] = forgottenLayer.flatMap {
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
          val point = Point(lon, lat)
          val wmPoint = point.reproject(latLngToWebMercator)
          val (x, y) = wmLayout.mapToGrid(wmPoint)
          (x, y, v)
        }
      }
    }

    val pipeline = ForgottenPopPipeline(
      "geom",
      URI.create(s"${outputUri.toString}/x$pixelScale"),
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
      .toDF("x", "y", "pop")
      .withColumn("geom", pipeline.geomUdf(col("h3Index")))
    VectorPipe(gridPointsDf, pipeline, vpOptions)
  }

  def persist: Unit = {
    grumpMaskRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
    forgottenLayer.persist(StorageLevel.MEMORY_AND_DISK_SER)
  }

  def unpersist: Unit = {
    forgottenLayer.unpersist()
    grumpMaskRdd.unpersist()
  }

  lazy val result: (PopulationSummary, StreamingHistogram) =
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

object PopulationNearRoadsLookupJob {

  @transient private[this] lazy val logger = getLogger

  /** Error prone process where we do the best we can (deprecated)
   * TopologyException exceptions happen on union
   */
  def unionAndKey(country: Country, geoms: Seq[Geometry], layout: LayoutDefinition): Seq[((Country, SpatialKey), Geometry)] = {
    try {
      val unionGeom: Geometry = CascadedPolygonUnion.union(geoms.asJava)
      val cogRegionKeys = layout.mapTransform.keysForGeometry(unionGeom)
      cogRegionKeys.map { cogKey =>
        ((country, cogKey), unionGeom)
      }
    } catch {
      case e: TopologyException =>
        val single = geoms.maxBy(_.getArea)
        logger.error(s"TopologyException on union keeping: " + single.toWKT)
        // we couldn't union them, lets get the one with biggest area
        val cogRegionKeys = layout.mapTransform.keysForGeometry(single)
        cogRegionKeys.map { cogKey =>
          ((country, cogKey), single)
        }
    }
  }.toSeq

  /** Geometries rasterized to masks, tiled by given layout
   * @note geoms and layout are expected to be in same CRS
   */
  def burnRoadMask(layout: LayoutDefinition, geoms: Seq[Geometry]): Seq[(SpatialKey, MutableArrayTile)] = {
    val options = Rasterizer.Options(includePartial =  false, sampleType = PixelIsPoint)
    val masks = mutable.HashMap.empty[SpatialKey, MutableArrayTile]
    val tileCols = layout.tileCols
    val tileRows = layout.tileRows
    for {
      geom <- geoms
      key <- layout.mapTransform.keysForGeometry(geom)
      extent = layout.mapTransform.keyToExtent(key)
      re = RasterExtent(extent, tileCols, tileRows)
      mask = masks.getOrElseUpdate(key, ArrayTile.empty(BitCellType, tileCols, tileRows))
    } Rasterizer.foreachCellByGeometry(geom, re, options) { (col, row) => mask.set(col, row, 1)}
    masks.map({ case (key, mask) => (key, mask)}).toList
  }

  /** Mutable, burn masks from right to left */
  def combineMasks(left: MutableArrayTile, right: MutableArrayTile): MutableArrayTile = {
    cfor(0)(_ < left.cols, _ + 1) { col =>
      cfor(0)(_ < left.rows, _ + 1) { row =>
        if (left.get(col, row) == 0)
          left.set(col, row, right.get(col, row))
      }
    }
    left
  }

  /** Returns all OSM road ways that match the filter function.
   * @param tile MapBox OSM VectorTile, assumed to have "osm" layer
   * @param filter filter function on highway tag and surface tag
   */
  def extractRoads(tile: VectorTile, filter: RoadTags => Boolean): Seq[MultiLineString] = {
    tile.layers("osm").lines.
      filter(f => filter(RoadTags(f.data))).
      map(f => MultiLineString(f.geom)) ++
    tile.layers("osm").multiLines.
      filter(f => filter(RoadTags(f.data))).
      map(_.geom)
  }
}