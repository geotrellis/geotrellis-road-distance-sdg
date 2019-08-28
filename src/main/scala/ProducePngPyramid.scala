package geotrellis.sdg

import cats.implicits._
import com.monovore.decline._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.geom.{Geometry => JTSGeometry}
import org.locationtech.jts.operation.union.UnaryUnionOp
import geotrellis.contrib.vlm.spark.SpatialPartitioner
import geotrellis.layer._
import geotrellis.proj4.WebMercator
import geotrellis.raster._
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.raster.reproject.Reproject
import geotrellis.spark.store.kryo._
import geotrellis.spark._
import geotrellis.spark.clip.ClipToGrid
import geotrellis.spark.pyramid._
import geotrellis.spark.store.s3._
import geotrellis.vector._

import collection.JavaConversions._
import scala.util.Properties

object ProducePngPyramid
    extends CommandApp(
      name = "Generate PNG Pyramid",
      header =
        "Generates a raster image layer of global world pop values masked against buffered roads",
      main = {
        val inputOrcPathOpt = Opts.argument[String]("inputOrcPath")
        val outputPathOpt = Opts.argument[String]("outputPath")

        val minZoomOpt = Opts
          .option[Int]("minzoom", help = "The min zoom of the pyramid to write")
          .withDefault(0)

        val partitionsOpt = Opts
          .option[Int]("partitions", help = "Number of partitions to use")
          .withDefault(64)

        (inputOrcPathOpt, outputPathOpt, minZoomOpt, partitionsOpt).mapN {
          (inputOrcPath, outputPath, minZoom, partitions) =>
            // Disable all JVM security checks, otherwise we get a number of
            // SessionHiveMetaStoreClient exceptions when we try to read orc files
            // THIS IS BAD and we should figure out a more specific cause and fix.
            System.setSecurityManager(null)

            val conf =
              new SparkConf()
                .setIfMissing("spark.master", "local[*]")
                .setAppName("World Pop Image Pyramid")
                .set("spark.serializer", classOf[KryoSerializer].getName)
                .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
                .set("spark.kryoserializer.buffer.max", "1g")
                .set("spark.executor.memory", "4g")
                .set("spark.driver.memory", "4g")
                .set("spark.executorEnv.AWS_REGION",
                     Properties.envOrElse("AWS_REGION", "us-east-1"))
                .set("spark.executorEnv.AWS_PROFILE",
                     Properties.envOrElse("AWS_PROFILE", "default"))

            implicit val ss = SparkSession.builder
              .config(conf)
              .enableHiveSupport
              .getOrCreate
              .withJTS

            try {
              val partitioner = SpatialPartitioner(partitions)

              val osmRoadsDF: DataFrame =
                ss.read.schema(BufferedRoadsSchema()).orc(inputOrcPath)

              val countryList = CountryDirectory.countries.map(_._2) // TODO: Replace with full list?
              val formatter =
                CountryFormatter(ss.sparkContext, countryList.toList)
              val countriesRDD: MultibandTileLayerRDD[SpatialKey] =
                formatter.readCountries
              val md: TileLayerMetadata[SpatialKey] = countriesRDD.metadata
              val transform = md.mapTransform

              val osmRoadsRdd: RDD[Feature[JTSGeometry, String]] =
                osmRoadsDF.rdd.map { geomRow =>
                  val geom = geomRow.getAs[JTSGeometry]("bufferedGeom")
                  val name = geomRow.getAs[String]("countryName")
                  Feature(geom, name)
                }

              val clippedGeoms: RDD[(SpatialKey, Feature[Geometry, String])] =
                ClipToGrid(osmRoadsRdd, md.layout)

              val groupedClippedGeoms
                : RDD[(SpatialKey, Iterable[Feature[Geometry, String]])] =
                clippedGeoms.groupByKey(partitioner)

              val joinedRdd
                : RDD[(SpatialKey,
                       (MultibandTile,
                        Option[Iterable[Feature[Geometry, String]]]))] =
                countriesRDD.leftOuterJoin(groupedClippedGeoms, partitioner)

              val options: Rasterizer.Options =
                Rasterizer.Options(true, PixelIsArea)

              val maskedRdd: RDD[(SpatialKey, Tile)] = joinedRdd.map {
                // Union geoms to a single geom, then burn them out of the
                // tile as NODATA values
                case (spatialKey, (multibandTile, Some(features))) => {
                  val tileExtent: Extent = transform(spatialKey)
                  val tile = multibandTile.band(0)
                  val geoms = features.map(_.geom).filter(_.isValid).toSeq
                  val geom: JTSGeometry = UnaryUnionOp.union(geoms)
                  val maskValue = 1
                  val geomMask =
                    geom.rasterizeWithValue(RasterExtent(tileExtent, tile),
                                            maskValue,
                                            IntConstantNoDataCellType,
                                            options)

                  (spatialKey, tile.localMask(geomMask.tile, maskValue, NODATA))
                }
                // Passthrough, we still want the tile even if there's no geoms to burn
                // out of it
                case (spatialKey, (multibandTile, None)) =>
                  (spatialKey, multibandTile.band(0))
              }

              val resampleOptions = Reproject.Options.DEFAULT
              val (targetZoom, tileLayerRdd) = TileLayerRDD(maskedRdd, md)
                .reproject(ZoomedLayoutScheme(WebMercator, 256),
                           resampleOptions)
              val pyramid =
                Pyramid.fromLayerRDD(tileLayerRdd, Some(targetZoom), None)
              pyramid.levels.foreach {
                case (zoom, tileRdd) => {
                  val colorRamp = ColorRamps.Magma
                  val histogram = tileRdd.histogram(colorRamp.numStops)
                  val colorMap = colorRamp.toColorMap(histogram)
                  val keyToPath = (k: SpatialKey) =>
                    s"${outputPath}/${zoom}/${k.col}/${k.row}.png"
                  val imageRdd = tileRdd
                    .map {
                      case (key, tile) => (key, tile.renderPng(colorMap).bytes)
                    }
                  SaveToS3(imageRdd, keyToPath)
                }
              }
            } finally {
              ss.sparkContext.stop
            }
        }
      }
    )
