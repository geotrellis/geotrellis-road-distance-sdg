package geotrellis.sdg

import geotrellis.vector._
import geotrellis.vectortile._
import geotrellis.proj4._
import geotrellis.proj4.util._

object GeometryUtils {
  final val notRoads = List("path", "steps", "bridleway", "footway")
  private def onlyHighways(feature: MVTFeature[Geometry]): Boolean = {
    feature.data.get("highway") match {
      case Some(v) => ! notRoads.contains(v)
      case None => false
    }
  }

  def extractAllRoads(tile: VectorTile ): Seq[MultiLineString] = {
    val mlines = tile.layers("osm").multiLines
    val lines = tile.layers("osm").lines
    // TODO: this is not all, I'm sure
    lines.filter(onlyHighways).map(f => MultiLineString(f.geom))++ mlines.filter(onlyHighways).map(_.geom)
  }

  /** Buffer geometry by meters by intermediate reprojection to the closest UTM zone */
  def bufferByMeters(geom: Geometry, sourceCrs: CRS, targetCrs: CRS, meters: Double): Geometry = {
    val utmZoneCrs = {
      val c = geom.getCentroid
      val center = Point(c.getX, c.getY).reproject(sourceCrs, LatLng)
      UTM.getZoneCrs(center.getX, center.getY)
    }
    val utmGeom = geom.reproject(sourceCrs, utmZoneCrs)
    utmGeom.buffer(meters).reproject(utmZoneCrs, targetCrs)
  }
}