package geotrellis.sdg

import java.net.URI

import geotrellis.layer._
import geotrellis.vector.{Extent, Feature, Geometry, Point}
import geotrellis.vectortile.VDouble
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, expr, udf}
import vectorpipe.vectortile._

/**
  * VectorPipe Pipeline to sum the population associated with an x/y coordinate
  * in the rasterLayoutScheme space. It outputs polygons that have the aggregated
  * value across all geometries under the polygon as a vector tile layer.
  *
  * The vector tile layer in the output is "data" and the population sum is stored
  * in the property "population" of each polygon geometry.
  *
  * The size of the output polygons is determined by the tileSize of the passed
  * rasterLayoutScheme. Larger tileSize == smaller polygon "pixels".
  *
  * This pipeline is fairly generic and could be re-used for any reduction
  * operation over a single value.
  */
case class ForgottenPopPipeline(geometryColumn: String,
                                baseOutputURI: URI,
                                rasterLayoutScheme: ZoomedLayoutScheme,
                                maxZoom: Int) extends Pipeline {

  override val layerMultiplicity: LayerMultiplicity = SingleLayer("data")

  def geomUdf(layout: LayoutDefinition) = udf { (x: Long, y: Long) =>
    val (xCoord, yCoord) = layout.gridToMap(x, y)
    Point(xCoord, yCoord)
  }

  override def reduce(input: DataFrame, layoutLevel: LayoutLevel, keyColumn: String): DataFrame = {
    // Since we call reduce before we generate VT for a layer, skip reduce on first zoom
    val reduceUdf = if (layoutLevel.zoom == maxZoom) {
      udf { c: Long => c }
    } else {
      udf { c: Long => c / 2 }
    }
    val rasterLayout = rasterLayoutScheme.levelForZoom(layoutLevel.zoom).layout
    input
      .select("x", "y", "pop")
      .withColumn("x", reduceUdf(col("x")))
      .withColumn("y", reduceUdf(col("y")))
      .groupBy(col("x"), col("y"))
      .agg(expr("sum(pop) as pop"))
      .withColumn("geom", geomUdf(rasterLayout)(col("x"), col("y")))
      .withColumn(keyColumn, keyTo(layoutLevel.layout)(col("geom")))
  }

  override def pack(row: Row, zoom: Int): VectorTileFeature[Geometry] = {
    val pop = row.getAs[Double]("pop")
    val center = row.getAs[Point]("geom")
    val layout = rasterLayoutScheme.levelForZoom(zoom).layout
    val xDistance = layout.cellSize.width
    val yDistance = layout.cellSize.height
    val geom = Extent(center.getX - xDistance / 2, center.getY - yDistance / 2,
                      center.getX + xDistance / 2, center.getY + yDistance / 2).toPolygon
    Feature(geom, Map("population" -> VDouble(pop)))
  }
}
