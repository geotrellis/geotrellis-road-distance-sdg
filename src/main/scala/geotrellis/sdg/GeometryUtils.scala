package geotrellis.sdg

import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.proj4.util._

object GeometryUtils {
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