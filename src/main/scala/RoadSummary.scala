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

import org.locationtech.geomesa.spark.jts._

import org.locationtech.jts.geom.{Geometry => JTSGeometry, Polygon}

import org.apache.commons.io.IOUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.jts.GeometryUDT

import cats.implicits._
import com.monovore.decline._


object RoadSummary extends CommandApp(
  name = "Road Population Summary",
  header = "Poduces population summary json",
  main = {
    val orcFile = Opts.option[String]("orc-file", help = "The path to the orc file that should be read")
    val countryFiles = Opts.options[String]("country", help = "The path to the country raster that should be read")
    val outputPath = Opts.option[String]("output", help = "The path that the output should be written to")

    (orcFile, countryFiles, outputPath).mapN { (targetFile, countryList, output) =>
      System.setSecurityManager(null)

      val conf =
        new SparkConf()
          .setIfMissing("spark.master", "local[*]")
          .setAppName("Road Summary")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
          .set("spark.executor.memory", "8g")
          .set("spark.driver.memory", "8g")
          .set("spark.default.parallelism", "120")

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      val sqlContext = ss.sqlContext

      ss.withJTS

      try {
        val schema =
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

        val osmData: DataFrame =
          ss
            .read
            .schema(schema)
            .orc(targetFile)

        val pavedRoads: DataFrame =
          osmData.where(osmData("surfaceType").isin(pavedSurfaces:_*))

        val pavedRoadsRDD: RDD[Feature[Geometry, Map[String, String]]] =
          pavedRoads.rdd.map { geomRow =>
            val geom = geomRow.getAs[JTSGeometry]("bufferedGeom")
            val name = geomRow.getAs[String]("countryName")

            Feature(geom, Map("name" -> name))
          }

        val formatter = CountryFormatter(ss.sparkContext, countryList.toList)

        val countriesRDD: MultibandTileLayerRDD[SpatialKey] =
          formatter.readCountries

        val md: TileLayerMetadata[SpatialKey] =
          countriesRDD.metadata

        val transform = MapKeyTransform(md.crs, md.layout.layoutCols, md.layout.layoutRows)

        val clippedGeoms: RDD[(SpatialKey, Feature[Geometry, Map[String, String]])] =
          ClipToGrid(pavedRoadsRDD, md.layout)

        val t = Transform(LatLng, WebMercator)

        val keys = clippedGeoms.keys.collect()

        val reprojectedKeys =
          keys.map { key =>
            Reproject(transform(key), t).getCentroid().toText()
          }

        println(s"\n\nThis is the extent of the layers: ${md.extent.toPolygon.toText()}\n")
        println(s"\n\nThis is the bounds for the layer: ${md.bounds}\n")

        reprojectedKeys.foreach { println }

        /*
        val groupedClippedGeoms: RDD[(SpatialKey, Iterable[Feature[Geometry, Map[String, String]]])] = clippedGeoms.groupByKey()

        val joinedRDD: RDD[(SpatialKey, (MultibandTile, Iterable[Feature[Geometry, Map[String, String]]]))] =
          countriesRDD.join(groupedClippedGeoms)

        println(s"\n\nThis is the count of the joinedRDD: ${joinedRDD.count}\n\n")

        val options: Rasterizer.Options = Rasterizer.Options(true, PixelIsArea)

        val calculatePop = (tile: Tile) => {
          var acc: Double = 0.0
          tile.foreachDouble { (d: Double) => if (!isNoData(d)) acc += d }
          acc
        }

        val countryPopulations: RDD[(String, Double)] =
          joinedRDD.flatMap { case (key, (tile, features)) =>
            val tileExtent: Extent = transform(key)
            val firstBand = tile.band(0)

            features.map { feat =>
              val maskedTile = firstBand.mask(tileExtent, feat.geom, options)
              val maskedPopulation = calculatePop(maskedTile)

              (feat.data.get("name").get, maskedPopulation)
            }
          }

        val reducedCountryPopulations: RDD[(String, Double)] =
          countryPopulations.reduceByKey { _ + _ }

        val mappedTotalPopulations: Map[String, Double] =
          formatter.mappedPopulations(countriesRDD)

        val rowRDD: RDD[Row] =
          reducedCountryPopulations.map { case (code, roadPopulation) =>
            val totalPopulation = mappedTotalPopulations.get(code).get

            Row(CountryDirectory.codeToName(code), code, totalPopulation, roadPopulation)
          }

        val outputSchema =
          new StructType()
            .add(StructField("country_name", StringType, nullable = false))
            .add(StructField("country_code", StringType, nullable = false))
            .add(StructField("total_population", DoubleType, nullable = false))
            .add(StructField("road_population", DoubleType, nullable = false))

        val populationDataFrame: DataFrame = ss.createDataFrame(rowRDD, outputSchema)

        println(s"\n\n${populationDataFrame.show()}\n\n")

        //populationDataFrame.write.json(output)
      */

      } finally {
        ss.sparkContext.stop
      }
    }
  }
)
