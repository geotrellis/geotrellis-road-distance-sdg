package geotrellis.sdg

import java.net.URI

import geotrellis.layer._
import geotrellis.proj4._
import geotrellis.proj4.util._
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
import org.locationtech.jts.geom.TopologyException
import org.locationtech.jts.operation.union.CascadedPolygonUnion
import org.log4s._
import spire.syntax.cfor._
import vectorpipe.VectorPipe

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * Here we're going to start from country mbtiles and then do ranged reads
  */
class PopulationNearRoadsJob(
  country: Country,
  grumpRdd: RDD[Geometry],
  layout: LayoutDefinition,
  crs: CRS,
  roadHistogram: RoadHistogram,
  roadFilter: RoadTags => Boolean
)(implicit spark: SparkSession) extends Serializable {
  import PopulationNearRoadsJob._

  @transient private[this] lazy val logger = getLogger

  @transient lazy val rasterSource = country.rasterSource

  @transient lazy val layoutTileSource: LayoutTileSource[SpatialKey] =
    LayoutTileSource.spatial(rasterSource, layout)

  val wsCountryBorder: Geometry = {
    // Buffer country border to avoid clipping out tiles where WorldPop and NaturalEarth don't agree on borders
    country.boundary.buffer(2).reproject(LatLng, layoutTileSource.source.crs)
  }
  val countryRdd: RDD[Country] = spark.sparkContext.parallelize(Array(country), 1)

  // Generate per-country COG regions we will need to read.
  // These do not contain duplicate reads of pixels - each key maps to COG segment
  val regionsRdd: RDD[(SpatialKey, Unit)] =
    countryRdd.flatMap({ country =>
      // WARN: who says these COGs exists there at all (USA does not) ?
      logger.info(s"Reading: $country ${layoutTileSource.source.name}")
      // Russian and USA rasters have large amount of NODATA regions that we want to clip to save cycles
      // if (country.code == "RUS" || country.code == "USA")
        layoutTileSource.layout.mapTransform.keysForGeometry(wsCountryBorder).map { key => (key, ())}
      // else
        // layoutTileSource.keys.map(key => (key, ()))
    }).setName(s"${country.code} Regions").cache()

  val partitioner: Partitioner = {
    val regionCount = regionsRdd.count
    val partitions = math.max(1, regionCount / 16).toInt
    logger.info(s"Partitioner: ${country.code} using $partitions partitions for $regionCount regions")
    //SpatialPartitioner(partitions, bits = 4)
    new HashPartitioner(partitions)
  }

  val roadsRdd: RDD[(SpatialKey, Seq[Feature[MultiLineString, RoadTags]])] = {
    val filter = { t: RoadTags => roadFilter(t) || t.isPossiblyMotorRoad }
    val allKeysRdd = countryRdd.flatMap { country =>
      val qaTiles = OsmQaTiles.fetchFor(country)
      qaTiles.allKeys.map { key => (key, Unit) }
    }.setName(s"${country.code} MBTile Keys").cache()

    allKeysRdd.partitionBy(partitioner).map { case (key, _) =>
      val qaTiles = OsmQaTiles.fetchFor(country)
      val row = qaTiles.fetchRow(key).get
      val roads = extractRoads(row.tile, filter)
      (key, roads)
    }
  }

    // Each buffered road "overflowed", we need to join it back up, not going to trim it tough
  val roadMaskRdd: RDD[(SpatialKey, MutableArrayTile)] = {
    roadsRdd
      .flatMap { case (_, roads) =>
        val buffered = roads
          .map{ road => // project to UTM and count
            val utmZoneCrs = {
              val center = road.getCentroid.reproject(WebMercator, LatLng)
              UTM.getZoneCrs(center.getX, center.getY)
            }
            val utmRoad = road.reproject(WebMercator, utmZoneCrs)
            val included = roadFilter(utmRoad.data)
            roadHistogram.add((utmRoad.geom.getLength() / 1000.0, included, utmRoad.data))
            // Buffer by 2KM to either size and reproject to match raster source
            val meters = 2000
            utmRoad.mapGeom(_.buffer(meters).reproject(utmZoneCrs, layoutTileSource.source.crs))
          }
          .filter(f => roadFilter(f.data))
          .map(_.geom)
        burnRoadMask(layoutTileSource.layout, buffered)
      }
      .reduceByKey(partitioner, (l, r) => combineMasks(l, r))
  }

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

  def roadLayerTiles(outputUri: URI, maxZoom: Int, minZoom: Int): Unit = {
    import spark.implicits._

    val filteredRoadsWithTagsRdd: RDD[(SpatialKey, MultiLineString, Long, String, String, Boolean)] =
      roadsRdd
        .flatMapValues(identity(_))
        .map { case (key: SpatialKey, feature: Feature[MultiLineString, RoadTags]) =>
          val tags = feature.data
          val isIncluded = roadFilter(tags)
          (key, feature.geom, tags.id.getOrElse(-1), tags.highway.getOrElse(""), tags.surface.getOrElse(""), isIncluded)
        }
    val filteredRoadsWithTagsDf = filteredRoadsWithTagsRdd
      .toDF("key", "geom", "osmId", "highway", "surface", "isIncluded")

    val pipeline = FilteredRoadsPipeline("geom", outputUri)
    val vpOptions = VectorPipe.Options(
      maxZoom = maxZoom,
      minZoom = Some(minZoom),
      srcCRS = WebMercator,
      destCRS = None,
      useCaching = false,
      orderAreas = false
    )

    VectorPipe(filteredRoadsWithTagsDf, pipeline, vpOptions)
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


object PopulationNearRoadsJob {

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
  def extractRoads(tile: VectorTile, filter: RoadTags => Boolean): Seq[Feature[MultiLineString, RoadTags]] = {
    tile.layers("osm").lines.
      filter(f => filter(RoadTags(f.data))).
      map(f => Feature(MultiLineString(f.geom), RoadTags(f.data))) ++
    tile.layers("osm").multiLines.
      filter(f => filter(RoadTags(f.data))).
      map(f => Feature(f.geom, RoadTags(f.data)))
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
