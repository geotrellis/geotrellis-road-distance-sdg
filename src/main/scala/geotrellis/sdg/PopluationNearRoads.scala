package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.polygonal.visitors._
import geotrellis.vector._
import geotrellis.vector.io.wkt._
import geotrellis.vectortile._
import geotrellis.proj4._
import geotrellis.layer._
import geotrellis.contrib.vlm._
import geotrellis.store.index.zcurve.Z2
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.operation.union.UnaryUnionOp
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import geotrellis.qatiles._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import cats.implicits._
import com.monovore.decline._
import com.typesafe.scalalogging.LazyLogging

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.duration._


/**
 * Summary of population within 2km of roads
 */
object PopulationNearRoads extends CommandApp(
  name = "Population Near Roads",
  header = "Summarize population within and without 2km of OSM roads",
  main = {
    val countryCodesOpt = Opts.options[String]("countries", help = "Country code to use for input")
    val outputPath = Opts.option[String]("output", help = "The path that the resulting orc fil should be written to")
    val partitions = Opts.option[Int]("partitions", help = "The number of Spark partitions to use").orNone

    (countryCodesOpt, outputPath, partitions).mapN { (countryCodes, output, partitionNum) =>

      // TODO: filter out USA because we don't have a COG for it (that's a mistake in pre-processing)

      val countries = countryCodes.map({ code => (code, Country.fromCode(code)) }).toList.toMap
      val badCodes = countries.filter({ case (_, v) => v.isEmpty }).keys
      require(badCodes.isEmpty, s"Bad country codes: ${badCodes}")

      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("PopulationNearRoads")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
        .set("spark.default.parallelism", partitionNum.getOrElse(120).toString)
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .set("spark.network.timeout", "12000s")
        .set("spark.executor.heartbeatInterval", "600s")

      implicit val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate

      try {
        spark.withJTS
        // USA is not present in the S3 bucket because of pre-processing error
        println(s"Partitions: $partitionNum")
        val job =  new PopulationNearRoadsJob(countries.values.flatten.toList.filter(_.code != "USA"), partitionNum)
        job.result.foreach(println)
      } finally {
        spark.stop
      }
    }

})

class PopulationNearRoadsJob(
  countries: List[Country],
  partitionNum: Option[Int]
)(implicit spark: SparkSession) extends LazyLogging with Serializable {
  // 1. Get a list of countries I'm going to use for input
  logger.info(s"INPUT: $countries")
  require(countries.nonEmpty, "Empty Input!")
  val countryRdd: RDD[Country] = spark.sparkContext.parallelize(countries, countries.length)

  // 2. Generate per-country COG regions we will need to read. These do not contain duplicate reads of pixels
  val regionsRdd: RDD[(Long, (Country, RasterRegion))] = countryRdd.
    flatMap({ country =>
      val countryBoundary: PreparedGeometry = {
        // prepare geometry to make the many intersection checks to come more efficient
        val factory = new PreparedGeometryFactory
        factory.create(country.feature.geom)
      }
      // WARN: who says these COGs exists there at all (USA does not) ?
      val rs = country.rasterSource

      // -- how big: Use 256x256 tiles to match block size of COGs
      val ts = LayoutTileSource.spatial(rs, LayoutDefinition(rs.gridExtent, 256))

      // -- intersect regions with country boundary to filter out regions covering NODATA (see russia COG)
      ts.keyedRasterRegions.
        filter({ case (key, region) =>
          countryBoundary.intersects(region.extent.toPolygon)
        }).
        map({ case (key, region) =>
          val index: Long = Z2(key.col, key.row).z
          (index, (country, region))
        })
    }).cache

  // 3. Partition the regions to balance the load, some countries are much larger than others

  val partitionedRdd: RDD[(Long, (Country, RasterRegion))] = {
    val regionCount: Long = regionsRdd.count
    val partitions: Long = math.max(regionCount / 512, 1)
    logger.info(s"Partitioning into $partitions from $regionCount regions")
    val partitioner = new RangePartitioner(partitions.toInt, rdd = regionsRdd)
    regionsRdd.partitionBy(partitioner)
  }

  // 4. Compute per region population summary
  val perRegionSummary: RDD[(Country, PopulationSummary)] = partitionedRdd.map({ case (_, (country, region)) =>
    // 4.1. Secure us OSM QA MapBox tiles per country
    // !! There can be multiple partitions on each executor, some of them for same country
    // -> Assume that there is one JVM per machine, use cached Futures to get only one

    // 3.1. Fetch the tile for the Region
    val regionSummary =
      region.raster match {
        case None =>
          PopulationSummary(country, 0, 0, 0)

        case Some(raster) if raster.tile.band(0).isNoDataTile =>
          PopulationSummary(country, 0, 0, 1)

        case Some(raster) =>
          // 3.2. If population is not empty, read VectorTiles that intersect it
          val futureQaTiles: Future[OsmQaTiles] = OsmQaTiles.fetchFor(country)
          var regionPopulation: Double = 0
          raster.tile.band(0).foreachDouble { p => if (isData(p)) regionPopulation += p }

          val futureSummary = futureQaTiles.map({ qaTiles =>
            val vectorTiles: Seq[VectorTile] = qaTiles.queryForLatLngExtent(raster.extent)
            // 3.3. Apply the filtering function to get list of geometries we're going to work with
            val geoms: Seq[MultiLineString] = vectorTiles.flatMap(GeometryUtils.extractAllRoads)
            logger.info(s"Got roads ${geoms.length} from tile")

            // 3.4. Buffer out geometries by 2km
            val bufferedGeom: Seq[Geometry] = geoms.map(GeometryUtils.bufferByMeters(_, LatLng, WebMercator, 20000))
            val unionGeom: Geometry = UnaryUnionOp.union(bufferedGeom.asJava)

            if (bufferedGeom.isEmpty) {
              logger.warn(s"No roads in ${region.extent.toPolygon.toWKT}")
              PopulationSummary(country, regionPopulation, 0, 1)
            } else if (unionGeom.isEmpty) {
              logger.warn("Buffered Union is empty")
              PopulationSummary(country, regionPopulation, 0, 1)
            } else {
              val res = raster.polygonalSummary(unionGeom, SumVisitor)
              res.toOption match {
                case Some(sumValue) =>
                  logger.info(s"Counted ${sumValue(0).value} in ${country}  people near roads")
                  PopulationSummary(country, regionPopulation, sumValue(0).value, 1)
                case None =>
                  PopulationSummary(country, regionPopulation, 0, 1)
              }
            }
          })

          // Largest file can take up to 10 minutes to download, expect to pay on initial request per country
          Await.result(futureSummary, 15.minutes)
      }

    (country, regionSummary)
  })

  // 5. reduce to combine partial results and collect (~250 countries max)
  val result: Array[PopulationSummary] = perRegionSummary.reduceByKey(_ combine _).values.collect()
}