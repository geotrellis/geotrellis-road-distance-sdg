package geotrellis.sdg

import java.io.PrintWriter
import java.net.URI

import geotrellis.raster._
import geotrellis.raster.summary.polygonal._
import geotrellis.raster.summary.polygonal.visitors._
import geotrellis.vector._
import geotrellis.vector.io.wkt._
import geotrellis.vectortile._
import geotrellis.proj4._
import geotrellis.layer._
import geotrellis.contrib.vlm._
import geotrellis.store.index.zcurve.Z2
import org.locationtech.geomesa.spark.jts._
import org.locationtech.jts.operation.union.UnaryUnionOp
import org.locationtech.jts.geom.prep.{PreparedGeometry, PreparedGeometryFactory}
import geotrellis.qatiles._
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import cats.implicits._
import com.monovore.decline._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.store.hadoop.util.HdfsUtils
import geotrellis.vector.io.json.JsonFeatureCollection
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.feature.FeatureCollection

import scala.concurrent._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._
import scala.concurrent.duration._


/**
 * Summary of population within 2km of roads
 */
object PopulationNearRoads extends CommandApp(
  name = "Population Near Roads",
  header = "Summarize population within and without 2km of OSM roads",
  main = {
    val countryCodesOpt = Opts.options[String]("country", short = "c", help = "Country code to use for input").
      withDefault(Country.allCountries.keys.toList.toNel.get)
    val outputPath = Opts.option[String]("output", help = "The path that the resulting orc fil should be written to")
    val partitions = Opts.option[Int]("partitions", help = "The number of Spark partitions to use").orNone

    (countryCodesOpt, outputPath, partitions).mapN { (countryCodes, output, partitionNum) =>
      val countries = countryCodes.map({ code => (code, Country.fromCode(code)) }).toList.toMap
      val badCodes = countries.filter({ case (_, v) => v.isEmpty }).keys
      require(badCodes.isEmpty, s"Bad country codes: ${badCodes}")

      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("PopulationNearRoads")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
        .set("spark.default.parallelism", partitionNum.getOrElse(120).toString)
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .set("spark.network.timeout", "12000s")
        .set("spark.executor.heartbeatInterval", "600s")

      implicit val spark = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      val missingCountries = List("USA", "CHN", "RUS", "KSV")
      try {
        spark.withJTS
        // USA is not present in the S3 bucket because of pre-processing error
        val filtered = countries.values.flatten.toList.filterNot(c => missingCountries.contains(c.code))
        val job =  new PopulationNearRoadsJob(filtered, partitionNum)
        job.result.foreach(println)

        import _root_.io.circe._
        import _root_.io.circe.generic.semiauto._
        import _root_.io.circe.syntax._

        val collection = JsonFeatureCollection()
        job.result.foreach { case (country, summary) =>
            val adminFeature = country.feature
            val f = Feature(adminFeature.geom, summary.toOutput(country).asJson)
//              Map(
//              "WB_A3" -> country.code,
//              "NAME" -> country.name,
//              "POP" -> summary.population.toString,
//              "POP_ROAD" -> summary.populationNearRoads.toString,
//              "PCT_SERVED" -> (summary.populationNearRoads / summary.population).toString
//            ))
            collection.add(f)
        }


//        val writer = new PrintWriter(new java.io.File(output))
//        writer.print(collection.asJson)
//        writer.close()

        val conf = spark.sparkContext.hadoopConfiguration
        val uri = new URI(output)
        val fs = FileSystem.get(uri, conf)
        val out = fs.create(new Path(output))
        try { out.writeChars(collection.asJson.spaces2) }
        finally { out.close() }

      } finally {
        spark.stop
      }
    }
})