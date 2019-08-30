package geotrellis.sdg

import geotrellis.vector._
import geotrellis.vectortile._
import geotrellis.proj4._
import geotrellis.proj4.util._

object GeometryUtils {
  def extractAllRoads(tile: VectorTile ): Seq[MultiLineString] = {
    val mlines = tile.layers("osm").multiLines
    val lines = tile.layers("osm").lines
    // TODO: this is not all, I'm sure
    lines.map(f => MultiLineString(f.geom)) ++ mlines.map(_.geom)
  }

  /** Buffer geometry by meters by intermediate reprojection to the closest UTM zone */
  def bufferByMeters(geom: Geometry, sourceCrs: CRS, targetCrs: CRS, meters: Double): Geometry = {
    val center = geom.getCentroid
    val utmZoneCrs = UTM.getZoneCrs(center.getX, center.getY)
    val utmGeom = geom.reproject(sourceCrs, utmZoneCrs)
    utmGeom.buffer(meters).reproject(utmZoneCrs, targetCrs)
  }
}