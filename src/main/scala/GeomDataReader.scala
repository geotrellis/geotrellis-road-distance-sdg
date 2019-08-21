package geotrellis.sdg

import geotrellis.proj4._
import geotrellis.proj4.util._
import geotrellis.vector.{Extent, Geometry}
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import geotrellis.layer._

import org.locationtech.geomesa.spark.jts._

import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import org.apache.commons.io.IOUtils

import org.apache.spark.sql.{SQLContext, DataFrame}
import org.apache.spark.sql.functions.{udf, explode, lit}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.net.URI
import java.util.zip.GZIPInputStream


object GeomDataReader {
  def readAndFormat(
    sqlContext: SQLContext,
    targetPath: String,
    countryCode: String
  ): DataFrame = {

    val transform = MapKeyTransform(WebMercator, 4096, 4096)

    // This DataFrame contains the vector tile data
    val tileDataFrame: DataFrame =
      sqlContext
        .read
        .format("jdbc")
        .options(
          Map(
            "url" -> s"jdbc:sqlite:$targetPath",
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

        // The Y's stored in the mbtiles are reveresed, so we need to flip them
        val flippedRow: Int = (1 << 12) - row - 1

        val tileExtent: Extent = transform(col, flippedRow)

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

          (feat.geom, featureType, roadType, surfaceType)
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

    val bufferGeoms: (Geometry) => Geometry =
      (geom: Geometry) => {
        val latLngTransform = Transform(WebMercator, LatLng)
        val latLngGeom = Reproject(geom, latLngTransform)

        //println(s"\nThis is the WebMercator centroid: ${geom.getCentroid().toText()}")

        val center = latLngGeom.getCentroid()
        val x = center.getX()
        val y = center.getY()

        //println(s"This is the latLng centroid reprojected from the WebMercator Geometry: ${center.toText()}")

        val utmCRS = UTM.getZoneCrs(x, y)
        val utmTransform = Transform(LatLng, utmCRS)

        val utmGeom = Reproject(latLngGeom, utmTransform)

        //println(s"This is the UTM centroid reprojected from the LatLng Geometry: ${utmGeom.getCentroid().toText()}")

        val bufferedUTMGeom = utmGeom.buffer(2.0)

        val backTransform = Transform(utmCRS, LatLng)

        val result = Reproject(bufferedUTMGeom, backTransform)

        //println(s"This is the UTM centroid being reprojected back to LatLng: ${result.getCentroid().toText()}\n")

        result
      }

    val bufferGeomsUDF = udf(bufferGeoms)

    filteredDataFrame
      .withColumn("bufferedGeom", bufferGeomsUDF(filteredDataFrame.col("geom")))
      .withColumn("countryName", lit(CountryDirectory.codeToName(countryCode)))
  }
}
