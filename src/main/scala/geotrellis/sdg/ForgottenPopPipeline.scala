package geotrellis.sdg

import collection.JavaConverters._
import java.net.URI

import com.uber.h3core._
import geotrellis.layer._
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.vectortile.VDouble
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, expr, udf}
import org.locationtech.jts.geom.Coordinate
import vectorpipe.vectortile._

/**
  * VectorPipe Pipeline to sum the population associated with an x/y coordinate
  * in the rasterLayoutScheme space. It outputs hex polygons that have the aggregated
  * value across all geometries under the polygon as a vector tile layer.
  *
  * The vector tile layer in the output is "data" and the population sum is stored
  * in the property "population" of each polygon geometry.
  *
  * This pipeline is fairly generic and could be re-used for any reduction
  * operation over a single value.
  */
case class ForgottenPopPipeline(geometryColumn: String,
                                baseOutputURI: URI,
                                maxZoom: Int) extends Pipeline {

  override val layerMultiplicity: LayerMultiplicity = SingleLayer("data")

  @transient lazy val h3 = H3Core.newInstance

  def geomUdf = udf { (h3Index: String) =>
    var hexagonCoords = h3
      .h3ToGeoBoundary(h3Index)
      .asScala
      .map { c => new Coordinate(c.lng, c.lat) }
      .toArray
    // Add start point to end of coord array, its not included in
    // h3 output and JTS linear rings expect it.
    hexagonCoords = hexagonCoords :+ hexagonCoords(0)
    val polygon = Polygon(hexagonCoords)
    polygon.reproject(LatLng, WebMercator)
  }

  override def reduce(input: DataFrame, layoutLevel: LayoutLevel, keyColumn: String): DataFrame = {
    // Since we call reduce before we generate VT for a layer, skip reduce on first zoom
    val reduceUdf = if (layoutLevel.zoom == maxZoom) {
      udf { h3Index: String => h3Index }
    } else {
      udf { h3Index: String =>
        val zoom = h3.h3GetResolution(h3Index)
        h3.h3ToParentAddress(h3Index, zoom - 1)
      }
    }
    input
      .select("h3Index", "pop")
      .withColumn("h3Index", reduceUdf(col("h3Index")))
      .groupBy(col("h3Index"))
      .agg(expr("sum(pop) as pop"))
      .withColumn("geom", geomUdf(col("h3Index")))
      .withColumn(keyColumn, keyTo(layoutLevel.layout)(col("geom")))
  }

  override def pack(row: Row, zoom: Int): VectorTileFeature[Geometry] = {
    val geom = row.getAs[Geometry]("geom")
    val pop = row.getAs[Double]("pop")
    Feature(geom, Map("population" -> VDouble(pop)))
  }
}
