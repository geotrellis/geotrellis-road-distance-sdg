package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector.{Extent, Geometry, Feature}
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import geotrellis.proj4._
import geotrellis.spark.MultibandTileLayerRDD
import geotrellis.spark.store.kryo._
import geotrellis.spark.clip._
import geotrellis.layer._

import geotrellis.contrib.vlm.spark.SpatialPartitioner

import org.locationtech.geomesa.spark.jts._

import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import org.apache.commons.io.IOUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.sql.functions.sum

import cats.implicits._
import com.monovore.decline._

import scala.collection.mutable.ListBuffer


object RoadSummary extends CommandApp(
  name = "Road Population Summary",
  header = "Poduces population summary json",
  main = {
    val orcFile = Opts.option[String]("orc", help = "The path to the orc file that contains the buffered OSM geometries")
    val countryFiles = Opts.options[String]("country", help = "The Alpha-3 code for a country from the ISO 3166 standard")
    val outputPath = Opts.option[String]("output", help = "The path that the resulting json should be written to")
    val partitions = Opts.option[Int]("partitions", help = "The number of Spark partitions to use").withDefault(120)

    (orcFile, countryFiles, outputPath, partitions).mapN { (targetFile, countryList, output, partitionNum) =>
      System.setSecurityManager(null)

      val conf =
        new SparkConf()
          .setIfMissing("spark.master", "local[*]")
          .setAppName("Road Summary")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
          .set("spark.executor.memory", "8g")
          .set("spark.driver.memory", "8g")
          .set("spark.default.parallelism", partitionNum.toString)

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      val sqlContext = ss.sqlContext

      ss.withJTS

      try {
        val inputSchema =
          new StructType()
            .add(StructField("zoom_level", IntegerType, nullable = false))
            .add(StructField("tile_column", IntegerType, nullable = false))
            .add(StructField("tile_row", IntegerType, nullable = false))
            .add(StructField("geom", GeometryUDT, nullable = false))
            .add(StructField("type", StringType, nullable = false))
            .add(StructField("roadType", StringType, nullable = false))
            .add(StructField("surfaceType", StringType, nullable = false))
            .add(StructField("bufferedGeom", GeometryUDT, nullable = false))
            .add(StructField("countryName", StringType, nullable = false))

        // OSM defines these surfaces as being paved.
        val pavedSurfaces: Array[String] =
          Array(
            "paved",
            "asphalt",
            "concrete",
            "concrete:lanes",
            "concrete:plates",
            "paving_stones",
            "sett",
            "unhewn_cobblestone",
            "cobblestone",
            "metal",
            "wood"
          )

        val partitioner = SpatialPartitioner(partitionNum)

        val osmData: DataFrame =
          ss
            .read
            .schema(inputSchema)
            .orc(targetFile)

        val pavedRoads: DataFrame =
          osmData.where(osmData("surfaceType").isin(pavedSurfaces:_*))

        val pavedRoadsRDD: RDD[Feature[Geometry, String]] =
          pavedRoads.rdd.map { geomRow =>
            val geom = geomRow.getAs[JTSGeometry]("bufferedGeom")
            val name = geomRow.getAs[String]("countryName")

            Feature(geom, name)
          }

        val formatter = CountryFormatter(ss.sparkContext, countryList.toList)

        val countriesRDD: MultibandTileLayerRDD[SpatialKey] =
          formatter.readCountries

        val md: TileLayerMetadata[SpatialKey] =
          countriesRDD.metadata

        val transform = md.mapTransform

        val clippedGeoms: RDD[(SpatialKey, Feature[Geometry, String])] =
          ClipToGrid(pavedRoadsRDD, md.layout)

        val groupedClippedGeoms: RDD[(SpatialKey, Iterable[Feature[Geometry, String]])] =
          clippedGeoms.groupByKey(partitioner)

        val joinedRDD: RDD[(SpatialKey, (MultibandTile, Iterable[Feature[Geometry, String]]))] =
          countriesRDD.join(groupedClippedGeoms, partitioner)

        val options: Rasterizer.Options = Rasterizer.Options(true, PixelIsArea)

        val calculatePop = (tile: Tile) => {
          var acc: Double = 0.0
          tile.foreachDouble { (d: Double) => if (!isNoData(d)) acc += d }
          acc
        }

        val countryRoadPopulations: RDD[(SpatialKey, (String, Double))] =
          joinedRDD.mapPartitions({ partition =>
            partition.flatMap { case (key, (tile, features)) =>
              val tileExtent: Extent = transform(key)
              val firstBand = tile.band(0)

              features.map { feat =>
                val maskedTile = firstBand.mask(tileExtent, feat.geom, options)
                val maskedPopulation = calculatePop(maskedTile)

                (key, (feat.data, maskedPopulation))
              }
            }
          }, preservesPartitioning = true)

        val aggregatedCountryRoadPopulations: RDD[(SpatialKey, ListBuffer[(String, Double)])] = {
          val emptyValue = ListBuffer.empty[(String, Double)]

          val addValue = (list: ListBuffer[(String, Double)], elem: (String, Double)) =>
            elem +=: list

          val mergeCollection = (l1: ListBuffer[(String, Double)], l2: ListBuffer[(String, Double)]) =>
            if (l1.size > l2.size) l1 ++: l2 else l2 ++: l1

          countryRoadPopulations.aggregateByKey(emptyValue)(addValue, mergeCollection)
        }

        val reducedCountryRoadPopulations: RDD[(SpatialKey, Map[String, Double])] =
          aggregatedCountryRoadPopulations.mapValues { listBuffer =>
            val groupedValues: Map[String, List[(String, Double)]] =
              listBuffer.toList.groupBy { _._1 }

            groupedValues.map { case (countryName, groupedPops) =>
              val reducedPops = groupedPops.map { _._2 }.reduce { _ + _ }

              (countryName, reducedPops)
            }
          }

        val mappedTotalPopulations: Map[String, Double] =
          formatter.mappedPopulations(countriesRDD)

        val rowRDD: RDD[Row] =
          reducedCountryRoadPopulations.flatMap { case (_, mappedRoadPopulations) =>
            mappedRoadPopulations.map { case (countryName, roadPopulation) =>

              val countryCode: String = CountryDirectory.nameToCode(countryName)
              val totalPopulation: Double = mappedTotalPopulations.get(countryCode).get
              Row(countryName, countryCode, totalPopulation, roadPopulation)
            }
          }

        val outputSchema =
          new StructType()
            .add(StructField("country_name", StringType, nullable = false))
            .add(StructField("country_code", StringType, nullable = false))
            .add(StructField("total_population", DoubleType, nullable = false))
            .add(StructField("road_population", DoubleType, nullable = false))

        val populationDataFrame: DataFrame = ss.createDataFrame(rowRDD, outputSchema)

        val reducedPopulationDataFrame: DataFrame =
          populationDataFrame
            .groupBy(
              populationDataFrame.col("country_name"),
              populationDataFrame.col("country_code"),
              populationDataFrame.col("total_population")
            ).agg(sum(populationDataFrame.col("road_population")).alias("road_population"))

        reducedPopulationDataFrame.write.json(output)

      } finally {
        ss.sparkContext.stop
      }
    }
  }
)
