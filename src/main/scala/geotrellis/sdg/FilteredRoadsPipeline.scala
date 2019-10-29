package geotrellis.sdg

import java.net.URI

import geotrellis.layer.LayoutLevel
import geotrellis.vector._
import geotrellis.vectortile._
import org.apache.spark.sql.{DataFrame, Row}
import vectorpipe.vectortile._

case class FilteredRoadsPipeline(geometryColumn: String,
                                 baseOutputURI: URI,
                                 reduceToIncludedZoom: Int) extends Pipeline {

  override val layerMultiplicity: LayerMultiplicity = SingleLayer("roads")

  override def reduce(input: DataFrame, layoutLevel: LayoutLevel, keyColumn: String): DataFrame = {
    if (reduceToIncludedZoom == layoutLevel.zoom) {
      input.where("isIncluded = true")
    } else {
      input
    }
  }

  override def pack(row: Row, zoom: Int): VectorTileFeature[Geometry] = {
    val geom = row.getAs[Geometry]("geom")
    val osmId = row.getAs[Long]("osmId")
    val highway = row.getAs[String]("highway")
    val surface = row.getAs[String]("surface")
    val isIncluded = row.getAs[Boolean]("isIncluded")

    Feature(geom, Map(
      "osmId" -> VInt64(osmId),
      "highway" -> VString(highway),
      "surface" -> VString(surface),
      "isIncluded" -> VBool(isIncluded)
    ))
  }
}

