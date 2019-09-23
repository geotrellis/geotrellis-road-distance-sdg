package geotrellis.sdg

import geotrellis.layer.{LayoutDefinition, LayoutTileSource, SpatialKey}
import geotrellis.raster.RasterSource
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.vector._
import geotrellis.shapefile._

import scala.collection.concurrent.TrieMap

case class Country(code: String) {
  @transient lazy val rasterSource: RasterSource = {
    val url = s"s3://azavea-worldpop/Population/Global_2000_2020/MOSAIC_2019/ppp_prj_2019_${code.toUpperCase()}.tif"
    Country.rasterSourceCache.getOrElseUpdate(url, GeoTiffRasterSource(url))
  }

  def feature: MultiPolygonFeature[Map[String, Object]] = {
    // Assume only fromString constructor has been used, so this is "safe"
    Country.allCountries(code)
  }

  def name: String = Country.allCountries(code).data("NAME_LONG").asInstanceOf[String]
}

object Country {
  @transient private lazy val rasterSourceCache = TrieMap.empty[String, RasterSource]

  val countries: Array[(String, String)] = {
    val lines = Resource.lines("countries.csv")
    lines
      .map { _.split(",").map { _.trim } }
      .map { arr =>
        val name = arr(0).toLowerCase.replaceAll(" ", "_")
        val code = arr(1).toUpperCase()

        (name, code)
      }.toArray
  }

  val allCountries: Map[String, MultiPolygonFeature[Map[String,Object]]] = {
    // this list was hand-curated to match WorldPop country code to QA tiles download

    val availableCodes = countries.map(_._2)
    val url = getClass.getResource("/ne_50m_admin_0_countries.shp")
    ShapeFileReader.readMultiPolygonFeatures(url).map { feature =>
      val isoCode = feature.data("SU_A3").asInstanceOf[String]
      (isoCode, feature)
    }.
      filter(_._1 != "-99").
      filter(r => availableCodes.contains(r._1)). // shapefile has too many administrative boundaries
      toMap
  }

  def slug(name: String): String = {
    name.toLowerCase.replace(' ', '_')
  }

  def fromCode(code: String): Option[Country] = {
    allCountries.get(code).map { country =>
      val code = country.data("SU_A3").asInstanceOf[String]
      Country(code.toUpperCase())
    }
  }

  def codeToName(code: String): String = {
    val filteredCountries: Array[(String, String)] =
      countries.filter { case (_, c) =>  c == code }

    if (filteredCountries.isEmpty)
      throw new Error(s"Could not find name for country code: $code")
    else
      filteredCountries.head._1
  }
}