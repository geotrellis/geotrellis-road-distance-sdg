package geotrellis.sdg

import java.io.PrintWriter
import java.net.URI

import geotrellis.vector._
import org.locationtech.geomesa.spark.jts._
import org.apache.spark._
import org.apache.spark.sql._
import cats.implicits._
import com.monovore.decline._
import com.typesafe.scalalogging.LazyLogging
import geotrellis.vector.io.json.JsonFeatureCollection
import org.apache.hadoop.fs.{FileSystem, Path}

import _root_.io.circe.syntax._
import cats.data.Validated


/**
  * Summary of population within 2km of roads
  */
object PopulationNearRoads extends CommandApp(
  name = "Population Near Roads",
  header = "Summarize population within and without 2km of OSM roads",
  main = {
    val countryCodesOpt = Opts.options[String](long = "country", short = "c",
      help = "Country code to use for input").
      withDefault(Country.allCountries.keys.toList.toNel.get).
      mapValidated({ codes =>
        codes.filter{ c => Country.fromCode(c).isEmpty } match {
          case Nil => Validated.valid(codes)
          case badCodes => Validated.invalidNel(s"Invalid countries: ${badCodes}")
        }
      })

    val excludeCodesOpt = Opts.options[String](long = "exclude", short = "x",
      help = "Country code to exclude from input").
      orEmpty

    val outputPath = Opts.option[String](long = "output",
      help = "The path/uri of the summary JSON file")

    val partitions = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    // TODO: Add .mbtiles URI and RasterSource URI to countries.csv
    // TODO: Add --playbook parameter that allows swapping countries.csv
    // TODO: Re-use road masking code to render WorldPop
    // TODO: Add option to save JSON without country borders

    (countryCodesOpt,excludeCodesOpt, outputPath, partitions).mapN {
      (countryCodes, excludeCodes, output, partitionNum) =>

      System.setSecurityManager(null)
      val conf = new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("PopulationNearRoads")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.kryo.registrator", "geotrellis.spark.store.kryo.KryoRegistrator")
        .set("spark.task.cpus", "1")
        .set("spark.default.parallelism", partitionNum.getOrElse(123).toString)
        .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35")
        .set("spark.network.timeout", "12000s")
        .set("spark.executor.heartbeatInterval", "600s")

      implicit val spark: SparkSession =
        SparkSession.builder.config(conf).enableHiveSupport.getOrCreate.withJTS

      try {
        val countries = countryCodes.toList.diff(excludeCodes).map({ code => Country.fromCode(code).get })
        val grumpUri = new URI("file:/Users/eugene/Downloads/grump-v1-urban-ext-polygons-rev01-shp/global_urban_extent_polygons_v1.01.shp")
        val grump = new Grump(grumpUri)

        val result =  PopulationNearRoadsJob(countries, grump, partitionNum)
        result.foreach(println)

        val collection = JsonFeatureCollection()
        result.foreach { case (country, summary) =>
            val adminFeature = country.feature
            val f = Feature(adminFeature.geom, OutputProperties(country, summary).asJson)
            collection.add(f)
        }

        // Write the result, works with local and remote URIs
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(new URI(output), conf)
        val pw = new PrintWriter(fs.create(new Path(output)))
        try {
          pw.print(collection.asJson.spaces2)
        }
        finally {
          pw.close()
          fs.close()
        }

      } finally {
        spark.stop
      }
    }
})