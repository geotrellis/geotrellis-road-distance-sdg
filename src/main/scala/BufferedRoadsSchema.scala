package geotrellis.sdg

import org.apache.spark.sql.jts.GeometryUDT
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object BufferedRoadsSchema {
  def apply(): StructType = {
    new StructType()
      .add(StructField("zoom_level", IntegerType, nullable = false))
      .add(StructField("tile_column", IntegerType, nullable = false))
      .add(StructField("tile_row", IntegerType, nullable = false))
      .add(StructField("geom", GeometryUDT, nullable = false))
      .add(StructField("type", StringType, nullable = false))
      .add(StructField("roadType", StringType, nullable = false))
      .add(StructField("surfaceType", StringType, nullable = false))
      .add(StructField("bufferedGeom", GeometryUDT, nullable = false))
      .add(StructField("countryName", StringType, nullable = false))
  }
}
