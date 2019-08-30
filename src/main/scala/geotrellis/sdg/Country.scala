package geotrellis.sdg

import geotrellis.contrib.vlm.gdal._
import geotrellis.contrib.vlm._
import geotrellis.contrib.vlm.geotiff.GeoTiffRasterSource
import geotrellis.vector._
import geotrellis.shapefile._

case class Country(
  code: String
) {
  def rasterSource: RasterSource =
    GeoTiffRasterSource(s"s3://azavea-worldpop/Population/Global_2000_2020/MOSAIC_2019/ppp_prj_2019_${code.toUpperCase()}.tif")

  def feature: MultiPolygonFeature[Map[String, Object]] = {
    // Assume only fromString constructor has been used, so this is "safe"
    Country.allCountries(code)
  }

  def name: String = Country.allCountries(code).data("NAME_LONG").asInstanceOf[String]
}

object Country {
  val allCountries: Map[String, MultiPolygonFeature[Map[String,Object]]] = {
    val url = getClass.getResource("/ne_50m_admin_0_countries.shp")
    ShapeFileReader.readMultiPolygonFeatures(url).map { feature =>
      val isoCode = feature.data("ISO_A3").asInstanceOf[String]
      (isoCode, feature)
    }.toMap
  }

  def slug(name: String): String = {
    name.toLowerCase.replace(' ', '_')
  }

  def fromCode(code: String): Option[Country] = {
    allCountries.get(code).map { country =>
      val code = country.data("ISO_A3").asInstanceOf[String]
      Country(code.toUpperCase())
    }
  }
}