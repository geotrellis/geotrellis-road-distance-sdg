package geotrellis.sdg

import com.monovore.decline.{CommandApp, Opts}
import org.apache.spark._
import org.apache.spark.sql._
import java.net.URI
import cats.implicits._
import geotrellis.raster.RasterSource
import geotrellis.layer.ZoomedLayoutScheme
import geotrellis.spark._
import geotrellis.proj4._
import geotrellis.layer._
import geotrellis.raster.resample.Bilinear

object RenderPyramidApp extends CommandApp(
  name = "RenderPyramidApp",
  header = "Render GeoTiff(s) as pyramid",
  main = {
    val rasterOpts = Opts.options[URI](
      long = "raster", short = "r",
      help = "Input raster")

    val tileLayerUriPrefixOpt = Opts.option[URI](
      long = "tileLayerUriPrefix",
      help = "URI prefix to write PNG tile layers to")

    val conf = new SparkConf()
      .setIfMissing("spark.master", "local[*]")
      .setAppName("RenderPyramidApp")

    /* --- Main --- */
    (rasterOpts, tileLayerUriPrefixOpt).mapN { (rasters, outputPrefixUri) =>
      implicit val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      implicit val sc = spark.sparkContext

      val baseZoom = 11
      val scheme = ZoomedLayoutScheme(WebMercator, 256)
      val layout: LayoutDefinition = scheme.levelForZoom(baseZoom).layout
      val rasterRdd = spark.sparkContext.parallelize(rasters.toList, rasters.length).
        map( raster => RasterSource(raster.toString).reproject(WebMercator))

      val layer = RasterSourceRDD.tiledLayerRDD(rasterRdd, layout, Bilinear).withContext(_.mapValues(_.band(0)))
      OutputPyramid.savePng(layer, SDGColorMaps.global, outputPrefixUri.toString)

      spark.stop
    }
})