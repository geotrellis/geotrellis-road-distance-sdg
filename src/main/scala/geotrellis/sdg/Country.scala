package geotrellis.sdg

import cats.data.{NonEmptyList, Validated}
import com.monovore.decline.Argument
import geotrellis.raster.RasterSource
import geotrellis.raster.geotiff.GeoTiffRasterSource
import geotrellis.vector._
import geotrellis.shapefile._
import cats.syntax.list._

import scala.collection.concurrent.TrieMap

trait Country {
  def name: String
  def boundary: MultiPolygon
  def code: String
  def rasterSource: RasterSource
}

case class CountryProvided(url: String) extends Country {
  @transient lazy val rasterSource: RasterSource = GeoTiffRasterSource(url)
  def name = url
  def code = "PROVIDED"
  def boundary: MultiPolygon = MultiPolygon(rasterSource.extent.toPolygon)
}

/**
 * @param name Country name, maps to MapBox QA tiles name
 * @param code three character letter country code used by WorldPop
 * @param domain code domain (ex: ISO_A3, SU_A3, WB_A3) used to lookup country boundary
 */
case class CountryLookup(name: String, code: String, domain: String = "SU_A3") extends Country {
  @transient lazy val rasterSource: RasterSource = {
    val base = "s3://azavea-worldpop/Population/Global_2000_2020/MOSAIC_2019"
    // WorldPop doesn't know about Somaliland
    val wpCode = if (code == "SOL") "SOM" else code
    val url = s"${base}/ppp_prj_2019_${wpCode}.tif"
    Country.rasterSourceCache.getOrElseUpdate(url, GeoTiffRasterSource(url))
  }

  def boundary: MultiPolygon = {
    // Assume only fromString constructor has been used, so this is "safe"
    Country.naturalEarthFeatures((code, domain))
  }

  def mapboxQaTilesUrl: String = {
    val base: String = "s3://mapbox/osm-qa-tiles-production/latest.country"
    val slug = name.toLowerCase.replace(" ", "_")
    s"${base}/${slug}.mbtiles.gz"
  }
}

object Country {
  @transient private[sdg] lazy val rasterSourceCache = TrieMap.empty[String, RasterSource]

  val all: NonEmptyList[Country] = {
    Resource.lines("countries.csv")
      .toList.toNel.get
      .map { _.split(",").map { _.trim } }
      .map { arr =>
        val name = arr(0)
        val code = arr(1).toUpperCase()
        val domain = arr(2).toUpperCase()
        CountryLookup(name, code, domain)
      }
  }

  val naturalEarthFeatures: Map[(String, String), MultiPolygon] = {
    val domains = List("ISO_A3", "SU_A3")
    def filterCode(code: String): Option[String] = if (code == "-99") None else Some(code)

    val url = getClass.getResource("/ne_50m_admin_0_countries.shp")
    ShapeFileReader.readMultiPolygonFeatures(url).flatMap { feature =>
      for {
        domain <- domains
        code <- filterCode(feature.data(domain).asInstanceOf[String])
      } yield ((code, domain), feature.geom)
    }.toMap
  }

  def lookupFromCode(code: String): Option[Country] = {
    all.find(_.code == code)
  }

  implicit val countryArgument: Argument[Country] = new Argument[Country] {
    def read(string: String): Validated[NonEmptyList[String], Country] = {
      if (string.length == 3)
        lookupFromCode(string) match {
          case Some(country) => Validated.valid(country)
          case None => Validated.invalidNel(s"Invalid country code: $string")
        }
      else
        Validated.valid(CountryProvided(string))
    }

    def defaultMetavar = "WorldPop Country Code"
  }
}