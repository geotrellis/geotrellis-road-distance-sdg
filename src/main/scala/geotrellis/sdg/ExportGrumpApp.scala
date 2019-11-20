package geotrellis.sdg

import java.net.URI

import com.monovore.decline.{CommandApp, Opts}
import geotrellis.layer.LayoutDefinition
import geotrellis.store.s3.{AmazonS3URI, S3ClientProducer}
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.spark._
import geotrellis.spark.stitch._
import geotrellis.vector.reproject._
import geotrellis.proj4._
import geotrellis.layer._
import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.spark.TileLayerRDD
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.storage.StorageLevel
import org.locationtech.geomesa.spark.jts._
import software.amazon.awssdk.services.s3.model.GetObjectRequest

object ExportGrumpApp extends CommandApp(
  name = "ExportGrumpApp",
  header = "Export GRUMP mask as GeoTiff",
  main = {
    val countryCodeOpt = Opts.option[Country](
      long = "country", short = "c",
      help = "Country code to use for input")

    countryCodeOpt.map { country =>
      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("ExportGrumpApp")

      implicit val spark: SparkSession =
        SparkSession.builder.config(conf).enableHiveSupport.getOrCreate.withJTS

      // This code is simplified duplication of logic from PopulationNearRoads

      val grumpUri = new URI("https://un-sdg.s3.amazonaws.com/data/grump-v1.01/global_urban_extent_polygons_v1.01.shp")
      val grump  = Grump(grumpUri)
      val grumpRdd: RDD[Geometry] = grump.readAll(256).persist(StorageLevel.MEMORY_AND_DISK_SER)
      val partitions = 256
      val partitioner = new HashPartitioner(partitions)
      val rasterSource = country.rasterSource
      val layout = LayoutDefinition(rasterSource.gridExtent, 256)

      val wsCountryBorder: Geometry = {
        // Buffer country border to avoid clipping out tiles where WorldPop and NaturalEarth don't agree on borders
        country.boundary.buffer(2).reproject(LatLng, rasterSource.crs)
      }

      val grumpMaskRdd: TileLayerRDD[SpatialKey] =
        Grump.masksForBoundary(grumpRdd, layout, wsCountryBorder, partitioner).
          withContext(_.filter{ case (key, _) => key.col >= 0 && key.row >= 0 }).
          withContext{ rdd =>
            val s = spark.sparkContext.parallelize(List(SpatialKey(0,0) -> BitConstantTile(0, 256, 256).asInstanceOf[Tile]))
            rdd union s
          }

      //val md = grumpMaskRdd.metadata.layout
      //grumpMaskRdd.foreach{ case (key, tile) =>
      //  val extent = key.extent(md)
      //    GeoTiff(tile, extent, LatLng).write(s"tiles/${key.col}_${key.row}.tif")
      //}

      val raster = geotrellis.spark.stitch.Implicits.withSpatialTileLayoutRDDMethods(grumpMaskRdd).sparseStitch().get
      GeoTiff(raster, LatLng).write(country.code + "_grump.tif" )

      spark.stop
    }
})