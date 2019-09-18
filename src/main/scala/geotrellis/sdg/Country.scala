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

  @transient lazy val tileSource: LayoutTileSource[SpatialKey] =
    LayoutTileSource.spatial(rasterSource, LayoutDefinition(rasterSource.gridExtent, 256))

  def feature: MultiPolygonFeature[Map[String, Object]] = {
    // Assume only fromString constructor has been used, so this is "safe"
    Country.allCountries(code)
  }

  def name: String = Country.allCountries(code).data("NAME_LONG").asInstanceOf[String]
}

object Country {
  @transient private lazy val rasterSourceCache = TrieMap.empty[String, RasterSource]

  val allCountries: Map[String, MultiPolygonFeature[Map[String,Object]]] = {
    // this list was hand-curated to match WorldPop country code to QA tiles download
    val availableCodes = CountryDirectory.countries.map(_._2)
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
}