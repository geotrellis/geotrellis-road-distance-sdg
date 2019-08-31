package geotrellis.sdg

import com.typesafe.scalalogging.LazyLogging
import geotrellis.contrib.vlm.{LayoutTileSource, RasterRegion}
import geotrellis.layer.{FloatingLayoutScheme, LayoutDefinition, SpatialKey}
import geotrellis.proj4.{LatLng, WebMercator}
import geotrellis.qatiles.OsmQaTiles
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster._
import geotrellis.raster.summary.polygonal.visitors.SumVisitor
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.polygonal.visitors._
import geotrellis.store.index.zcurve.Z2
import geotrellis.vector._
import geotrellis.vectortile.VectorTile
import org.apache.spark.RangePartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.locationtech.jts.geom.TopologyException
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import org.locationtech.jts.operation.union.{CascadedPolygonUnion, UnaryUnionOp}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.control.NonFatal
import spire.syntax.cfor._


/**
  * Here we're going to start from country mbtiles and then do ranged reads
  * @param countries
  * @param partitionNum
  * @param spark
  */
class PopulationNearRoadsJob(
  countries: List[Country],
  partitionNum: Option[Int]
)(implicit spark: SparkSession) extends LazyLogging with Serializable {
  import PopulationNearRoadsJob._
  // 1. Get a list of countries I'm going to use for input
  logger.info(s"INPUT: $countries - ${countries.length}" )
  logger.info(s"Partitions: ${partitionNum}")
  require(countries.nonEmpty, "Empty Input!")
  val countryRdd: RDD[Country] = spark.sparkContext.parallelize(countries, countries.length)

  val partialRoadMaskRdd: RDD[((Country, SpatialKey), MutableArrayTile)] =
    countryRdd.flatMap({ country =>
      val qaTiles = OsmQaTiles.fetchFor(country)
      qaTiles.allTiles.flatMap { row =>
        val roads: Seq[MultiLineString] = GeometryUtils.extractAllRoads(row.tile)
        // we're going to do summary in LatLng, so lets record buffered in that
        val buffered = roads.map(GeometryUtils.bufferByMeters(_, WebMercator, LatLng, meters = 2000))
        burnRoadMask(country, buffered)
      }
    })


  // Each buffered road "overflowed", we need to join it back up, not going to trim it tough
  val roadMaskRdd: RDD[((Country, SpatialKey), MutableArrayTile)] =
    partialRoadMaskRdd.reduceByKey(combineMasks)


  // We have RDD of countries and COG keys we will need to read.
  // However, there could be pop regions NOT covered by COGs that we still need to read
  // So we should consider all regions available form WorldPop and join them to vectors

  // 2. Generate per-country COG regions we will need to read. These do not contain duplicate reads of pixels
  val regionsRdd: RDD[((Country, SpatialKey), RasterRegion)] =
    countryRdd.flatMap({ country =>
      // WARN: who says these COGs exists there at all (USA does not) ?
      logger.info(s"Reading: $country ${country.rasterSource.name}")
      val countryBoundary = country.feature.geom

      // -- intersect regions with country boundary to filter out regions covering NODATA (see russia COG)
      try {
        country.tileSource.keyedRasterRegions.
          filter({ case (_, region) =>
            countryBoundary.intersects(region.extent.toPolygon)
          }).
          map({ case (key, region) =>
            ((country, key), region)
          })
      } catch {
        case NonFatal(e) =>
          logger.error(s"Failed reading $country ${country.rasterSource.name}")
          // Raster doesn't exist, so we can't do anything with it anyway, lets carry on
          List.empty
      }
    })

  // We already read and simplified the geometries, HashPartitioner is great to read COGs
  val joined = regionsRdd.leftOuterJoin(roadMaskRdd).cache
  val partitionCount: Int = math.max(1, partitionNum.getOrElse((regionsRdd.count / 32).toInt))
  val repartitioned = joined.repartition(partitionCount)


  // Now just iterate over the pairs and count up per region summary
  val popSummary: RDD[(Country, PopulationSummary)] =
    repartitioned.map { case ((country, key), (region, maybeRoadMask)) =>
     (region.raster, maybeRoadMask) match {
        case (None, _) =>
          country -> PopulationSummary(0, 0, 0)

        case (Some(raster), None) =>
          var regionPop: Double = 0
          raster.tile.band(0).foreachDouble(p => if (isData(p)) regionPop += p)
          country -> PopulationSummary(regionPop, 0, 1)

        case (Some(raster), Some(roadMask)) =>
          (country, maskSummary(raster.tile.band(0), roadMask))
      }
    }

  val result: Map[Country, PopulationSummary] = popSummary.reduceByKey(_ combine _).collect.toMap
}

object PopulationNearRoadsJob extends LazyLogging {

  /** Error prone process where we do the best we can */
  def unionAndKey(country: Country, geoms: Seq[Geometry]): Seq[((Country, SpatialKey), Geometry)] = {
    try {
      val unionGeom: Geometry = CascadedPolygonUnion.union(geoms.asJava)
      val cogRegionKeys = country.tileSource.layout.mapTransform.keysForGeometry(unionGeom)
      cogRegionKeys.map { cogKey =>
        ((country, cogKey), unionGeom)
      }
    } catch {
      case e: TopologyException =>
        val single = geoms.sortBy(_.getArea).last
        logger.error(s"TopologyException on union keeping: " + single.toWKT)
        // we couldn't union them, lets get the one with biggest area
        val cogRegionKeys = country.tileSource.layout.mapTransform.keysForGeometry(single)
        cogRegionKeys.map { cogKey =>
          ((country, cogKey), single)
        }
    }
  }.toSeq

  def burnRoadMask(country: Country, geoms: Seq[Geometry]): Seq[((Country, SpatialKey), MutableArrayTile)] = {
    val options = Rasterizer.Options(includePartial =  false, sampleType = PixelIsArea)
    val masks = mutable.HashMap.empty[SpatialKey, MutableArrayTile]
    val tileCols = country.tileSource.layout.tileCols
    val tileRows = country.tileSource.layout.tileRows
    for {
      geom <- geoms
      key <- country.tileSource.layout.mapTransform.keysForGeometry(geom)
      extent = country.tileSource.layout.mapTransform.keyToExtent(key)
      re = RasterExtent(extent, tileCols, tileRows)
      mask = masks.getOrElseUpdate(key, ArrayTile.empty(BitCellType, tileCols, tileRows))
    } Rasterizer.foreachCellByGeometry(geom, re, options) { (col, row) => mask.set(col, row, 1)}
    masks.map({ case (key, mask) => ((country, key), mask)}).toList
  }

  /** Mutable, burn masks from right to left */
  def combineMasks(left: MutableArrayTile, right: MutableArrayTile): MutableArrayTile = {
    cfor(0)(_ < left.cols, _ + 1) { col =>
      cfor(0)(_ < left.rows, _ + 1) { row =>
        if (isNoData(left.get(col, row)))
          left.set(col, row, right.get(col, row))
      }
    }
    left
  }

  def maskSummary(pop: Tile, mask: Tile): PopulationSummary = {
    require(pop.dimensions == mask.dimensions, s"pop: ${pop.dimensions} mask: ${mask.dimensions}")
    var totalPop = 0.0
    var roadPop = 0.0

    cfor(0)(_ < pop.cols, _ + 1) { col =>
      cfor(0)(_ < pop.rows, _ + 1) { row =>
        val p = pop.getDouble(col, row)

        if (isData(p)) {
          totalPop += p
          val m = mask.get(col, row)
          if (isData(m)) roadPop +=p
        }
      }
    }

    PopulationSummary(totalPop, roadPop, 1)
  }
}