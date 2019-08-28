package geotrellis.sdg

import geotrellis.proj4._
import geotrellis.proj4.util._
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.vectortile._
import geotrellis.layer._
import geotrellis.spark.store.kryo._

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


object GlobalRoadMask extends CommandApp(
  name = "Global Road Masking",
  header = "Poduces road geometries from OSM data for the world as an orc file",
  main = {
    val outputPath = Opts.option[String]("output", help = "The path that the resulting orc fil should be written to")
    val partitions = Opts.option[Int]("partitions", help = "The number of Spark partitions to use").withDefault(120)

    (outputPath, partitions).mapN { (output, partitionNum) =>
      System.setSecurityManager(null)

      val conf =
        new SparkConf()
          .setIfMissing("spark.master", "local[*]")
          .setAppName("Road Mask")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
          .set("spark.default.parallelism", partitionNum.toString)
          .set("spark.driver.memoryOverhead", "16G")
          .set("spark.executor.memoryOverhead", "16G")
          .set("spark.driver.memory", "32G")
          .set("spark.executor.memory", "32G")
          .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
          .set("spark.network.timeout", "12000s")
          .set("spark.executor.heartbeatInterval", "600s")

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      implicit val sqlContext = ss.sqlContext

      ss.withJTS

      try {
        val noUS = CountryDirectory.countries.filter { case (_, code) => code != "usa" }
        val groupedCountries: Iterator[Array[(String, String)]] = noUS.grouped(5)

        val dataFrames =
          groupedCountries.map { countries =>
            Work.produceGlobalMask(countries, partitionNum)
          }

        //val osmRoads: DataFrame = dataFrames.reduce { _ union _ } //Work.produceGlobalMask(countries, partitionNum)

        dataFrames.foreach { osmRoads =>
          osmRoads
            .write
            .mode("append")
            .partitionBy("countryName", "countryCode")
            .format("orc")
            .save(output)

          osmRoads.unpersist()
        }

        //val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
        /*
        CountryDirectory.countries.foreach { case (countryName, countryCode) =>
          val file = new File(s"/tmp/${countryName}.mbtiles")

          if (!file.exists())
            MbTilesDownloader.download(countryName)
        }

        val filteredCountries: Array[(String, String)] =
          CountryDirectory.countries.filter { case (_, code) => code != "usa" }

        val countriesRDD: RDD[(String, String)] =
          ss.sparkContext.parallelize(filteredCountries, partitionNum)

        val targetLayout = ZoomedLayoutScheme(WebMercator)

        val rowRDD: RDD[Row] =
          countriesRDD.flatMap { case (name, code) =>
            val file = new File(s"/tmp/${name}.mbtiles")

            logger.info(s"\n\nDownloading this country: $name\n")

            if (!file.exists())
              MbTilesDownloader.download(name)

            val mbtiles = new MbTiles(file.toString, targetLayout)
            val vectorTiles: Seq[VectorTile] = mbtiles.all(12)

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

        val osmRoads: DataFrame =
          filteredDataFrame
            .withColumn("bufferedGeom", bufferGeomsUDF(filteredDataFrame.col("geom")))

        val osmRoads: Array[DataFrame] =
          filteredCountries.map { case (countryName, countryCode) =>
            //MbTilesDownloader.download(countryName)

            val fileName: String = s"/tmp/${countryName}.mbtiles"
            //val filePath = new Path(fileName)

            val result: DataFrame = MbTilesReader.readAndFormat(fileName, countryCode, partitionNum)

            //fs.delete(filePath)

            result
          }

        val globalOSMRoads: DataFrame = osmRoads.reduce { _ union _ }

        osmRoads
          .write
          .format("orc")
          .save(output)
        */

      } finally {
        ss.sparkContext.stop
      }
    }
  }
)
