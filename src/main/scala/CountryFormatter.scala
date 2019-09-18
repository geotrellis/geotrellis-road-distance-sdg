package geotrellis.sdg

import geotrellis.layer._
import geotrellis.raster._
import geotrellis.raster.summary.polygonal.visitors.SumVisitor
import geotrellis.spark.{MultibandTileLayerRDD, RasterSourceRDD, RasterSummary}
import geotrellis.spark.summary.polygonal._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


// TODO: Come up with better name for this class
case class CountryFormatter(sc: SparkContext, countryCodes: List[String]) {
  def readCountries: MultibandTileLayerRDD[SpatialKey] = {
    val rasterSources: Seq[RasterSource] = mappedRasterSources.values.toSeq

    val rasterSourcesRDD: RDD[RasterSource] =
      sc.parallelize(rasterSources, rasterSources.size)

    val summary = RasterSummary.fromRDD(rasterSourcesRDD)
    val layout = summary.layoutDefinition(FloatingLayoutScheme())

    RasterSourceRDD.tiledLayerRDD(rasterSourcesRDD, layout)(sc)
  }

  private val mappedRasterSources: Map[String, RasterSource] =
    countryCodes.map { countryCode =>
      val path = s"${CountryFormatter.BASE_FILE_PATH}${countryCode.toUpperCase}.tif"

      countryCode -> RasterSource(path)
    }.toMap

  // Calculates the total population of a country using the country's Extent.
  def mappedPopulations(countries: MultibandTileLayerRDD[SpatialKey]): Map[String, Double] =
    mappedRasterSources.map { case (code, rasterSource) =>
      code -> countries.polygonalSummaryValue(rasterSource.extent.toPolygon, SumVisitor).toOption.get.head.value
    }
}

object CountryFormatter {
  final val BASE_FILE_PATH: String = "s3://azavea-worldpop/Population/Global_2000_2020/MOSAIC_2019/ppp_prj_2019_"
}
