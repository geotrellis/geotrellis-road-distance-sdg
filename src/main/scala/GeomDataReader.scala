package geotrellis.sdg

import geotrellis.proj4._
import geotrellis.proj4.util._
import geotrellis.vector.{Extent, Geometry}
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import geotrellis.spark._
import geotrellis.spark.io.kryo._
import geotrellis.spark.tiling._

import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions

import com.vividsolutions.jts.geom.{Geometry => JTSGeometry}

import org.apache.commons.io.IOUtils

import org.apache.spark.sql.{SQLContext, DataFrame}
//import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, explode}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI
import java.util.zip.GZIPInputStream


object GeomDataReader {
  def readAndFormat(sqlContext: SQLContext, transform: MapKeyTransform): DataFrame = {

    // This DataFrame contains the vector tile data
    val tileDataFrame: DataFrame =
      sqlContext
        .read
        .format("jdbc")
        .options(
          Map(
            "url" -> "jdbc:sqlite:/tmp/aland.mbtiles",
            "driver" -> "org.sqlite.JDBC",
            "dbtable" -> "tiles"
          )
        ).load()

    // The data is sotred as pbf files, which are gziped vectortiles.
    // So we need to decompress the bytes in order to read them in.
    val decompressBytes: (Array[Byte]) => Array[Byte] =
      (compressedBytes: Array[Byte]) => {
        val stream = new GZIPInputStream(new ByteArrayInputStream(compressedBytes))
        val output = new ByteArrayOutputStream()

        val result = {
          IOUtils.copy(stream, output)
          output.toByteArray
        }

        stream.close
        output.close

        result
      }

    val decompressBytesUDF = udf(decompressBytes)

    val uncompressedTileData: DataFrame =
      tileDataFrame.withColumn("uncompressed_data", decompressBytesUDF(tileDataFrame.col("tile_data")))

    val produceData: (Array[Byte], Int, Int) => Array[(JTSGeometry, String, String, String)] =
      (bytes: Array[Byte], col: Int, row: Int) => {
        val tileExtent: Extent = transform(col, row)

        val vectorTile = VectorTile.fromBytes(bytes, tileExtent)

        vectorTile.toIterable.toArray.map { feat =>
          val featureType = feat.data.get("@type").get.asInstanceOf[VString].value

          val roadType =
            feat.data.get("highway") match {
              case Some(value) => value.asInstanceOf[VString].value
              case None => null
            }

          val surfaceType =
            feat.data.get("surface") match {
              case Some(value) => value.asInstanceOf[VString].value
              case None => null
            }

          (feat.geom.jtsGeom, featureType, roadType, surfaceType)
        }
      }

    val produceDataUDF = udf(produceData)

    val geomDataFrame =
      uncompressedTileData
        .withColumn(
          "geom_data",
          produceDataUDF(
            uncompressedTileData.col("uncompressed_data"),
            uncompressedTileData.col("tile_column"),
            uncompressedTileData.col("tile_row")
          )
        )

    val explodedDataFrame =
      geomDataFrame
        .withColumn("data", explode(geomDataFrame.col("geom_data")))
        .select("zoom_level", "tile_column", "tile_row", "data.*")
        .withColumnRenamed("_1", "geom")
        .withColumnRenamed("_2", "type")
        .withColumnRenamed("_3", "roadType")
        .withColumnRenamed("_4", "surfaceType")

    val isValid: (JTSGeometry) => Boolean = (jtsGeom: JTSGeometry) => jtsGeom.isValid()

    val isValidUDF = udf(isValid)

    val filteredDataFrame: DataFrame =
      explodedDataFrame
        .where(
          isValidUDF(explodedDataFrame("geom")) &&
          explodedDataFrame("type") === "way" &&
          (explodedDataFrame("roadType").isNotNull && explodedDataFrame("surfaceType").isNotNull)
        )

    val bufferGeom: (JTSGeometry) => JTSGeometry =
      (geom: JTSGeometry) => {
        val latLngTransform = Transform(WebMercator, LatLng)
        val latLngGeom = Reproject(Geometry(geom), latLngTransform)

        val center = latLngGeom.jtsGeom.getCentroid()
        val x = center.getX()
        val y = center.getY()

        val utmCRS = UTM.getZoneCrs(x, y)
        val utmTransform = Transform(WebMercator, utmCRS)

        val utmGeom = Reproject(Geometry(geom), utmTransform)

        utmGeom.jtsGeom.buffer(2.0)
      }

    val bufferGeomUDF = udf(bufferGeom)

    filteredDataFrame
      .withColumn("bufferGeom", filteredDataFrame.col("geom"))
  }
}
