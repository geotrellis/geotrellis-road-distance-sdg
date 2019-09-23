package geotrellis.sdg2

import geotrellis.proj4._
import geotrellis.proj4.util._
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import geotrellis.layer._
import geotrellis.spark.store.kryo._
import geotrellis.qatiles.MbTiles

import org.locationtech.geomesa.spark.jts._

import org.locationtech.jts.geom.{Geometry => JTSGeometry}

import org.apache.spark.SparkConf
import org.apache.spark.storage._
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{udf, explode, lit}
import org.apache.spark.sql.jts.GeometryUDT

import org.apache.hadoop.fs.{FileSystem, Path}

import com.typesafe.scalalogging.LazyLogging

import cats.implicits._
import com.monovore.decline._

import java.io.File


object Work extends LazyLogging {
  def produceGlobalMask(countries: Array[(String, String)], partitionNum: Int)(implicit ss: SparkSession): DataFrame = {
    val countriesRDD: RDD[(String, String)] =
      ss.sparkContext.parallelize(countries, partitionNum)

    val targetLayout = ZoomedLayoutScheme(WebMercator)

    val rowRDD: RDD[Row] =
      countriesRDD.flatMap { case (name, code) =>
        val file = new File(s"/tmp/${name}.mbtiles")

        logger.info(s"\n\nDownloading this country: $name\n")

        //MbTilesDownloader.download(name)

        val mbtiles = new MbTiles(file, targetLayout)
        val vectorTiles: Iterator[VectorTile] = mbtiles.allTiles(12).map(_.tile)

        vectorTiles.flatMap { vectorTile =>
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
                case None => "null"
              }

            Row(name, code, feat.geom.asInstanceOf[JTSGeometry], featureType, roadType, surfaceType)
          }
        }
      }

    val dfSchema =
      new StructType()
        .add(StructField("countryName", StringType, nullable = false))
        .add(StructField("countryCode", StringType, nullable = false))
        .add(StructField("geom", GeometryUDT, nullable = false))
        .add(StructField("type", StringType, nullable = false))
        .add(StructField("roadType", StringType, nullable = true))
        .add(StructField("surfaceType", StringType, nullable = false))

    val geomDataFrame: DataFrame = ss.createDataFrame(rowRDD, dfSchema)

    rowRDD.unpersist()

    val isValidUDF = udf((jtsGeom: JTSGeometry) => jtsGeom.isValid())

    val filteredDataFrame: DataFrame =
      geomDataFrame
        .where(
          isValidUDF(geomDataFrame("geom")) &&
          geomDataFrame("roadType").isNotNull
        )

    val bufferGeomsUDF =
      udf((geom: Geometry) => {
        val latLngTransform = Transform(WebMercator, LatLng)
        val latLngGeom = Reproject(geom, latLngTransform)

        val center = latLngGeom.getCentroid()
        val x = center.getX()
        val y = center.getY()

        val utmCRS = UTM.getZoneCrs(x, y)
        val utmTransform = Transform(LatLng, utmCRS)

        val utmGeom = Reproject(latLngGeom, utmTransform)

        val bufferedUTMGeom = utmGeom.buffer(2.0)

        val backTransform = Transform(utmCRS, LatLng)

        Reproject(bufferedUTMGeom, backTransform)
      })

    filteredDataFrame
      .withColumn("bufferedGeom", bufferGeomsUDF(filteredDataFrame.col("geom")))
  }
}
