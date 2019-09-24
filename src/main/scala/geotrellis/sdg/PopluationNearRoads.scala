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
import geotrellis.layer.LayoutDefinition
import geotrellis.proj4.LatLng
import org.apache.spark.storage.StorageLevel

import scala.concurrent.{Await, Future}
import cats.implicits._
import cats.syntax.list._
import geotrellis.raster.io.geotiff.compression.DeflateCompression
import geotrellis.raster.io.geotiff.{GeoTiffBuilder, GeoTiffOptions, Tags, Tiled}


/**
  * Summary of population within 2km of roads
  */
object PopulationNearRoads extends CommandApp(
  name = "Population Near Roads",
  header = "Summarize population within and without 2km of OSM roads",
  main = {
    val countryOpt = Opts.options[Country](long = "country", short = "c",
      help = "Country code to use for input").
      withDefault(Country.all)

    val excludeOpt = Opts.options[Country](long = "exclude", short = "x",
      help = "Country code to exclude from input").
      orEmpty

    val outputPath = Opts.option[String](long = "output",
      help = "The path/uri of the summary JSON file")

    val partitions = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    // TODO: Add --playbook parameter that allows swapping countries.csv
    // TODO: Add option to save JSON without country borders

    (countryOpt, excludeOpt, outputPath, partitions).mapN {
      (countriesInclude, excludeCountries, output, partitionNum) =>

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
        val countries = countriesInclude.toList.diff(excludeCountries)

        val grumpUri = new URI("https://un-sdg.s3.amazonaws.com/data/grump-v1.01/global_urban_extent_polygons_v1.01.shp")
        val grump  = Grump(grumpUri)
        val grumpRdd = grump.readAll(256).persist(StorageLevel.MEMORY_AND_DISK_SER)

        import scala.concurrent.ExecutionContext.Implicits.global

        val result: Map[Country, PopulationSummary] =  {
          val future = Future.traverse(countries)( country => Future {
            spark.sparkContext.setJobGroup(country.code, country.name)

            val rasterSource = country.rasterSource
            val layout = LayoutDefinition(rasterSource.gridExtent, 256)

            val job = new PopulationNearRoadsJob(country, grumpRdd, layout, LatLng)
            job.grumpMaskRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
            job.forgottenLayer.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val (summary, histogram) = job.result

            /** Save country as GeoTIFF for inspection */
            //val builder = GeoTiffBuilder.singlebandGeoTiffBuilder
            //val md = job.forgottenLayer.metadata
            //val segments = job.forgottenLayer.collect().toMap
            //val tile = builder.makeTile(segments.toIterator, layout.tileLayout, md.cellType, Tiled(256), DeflateCompression)
            //val extent = md.layout.extent
            //val geotiff = builder.makeGeoTiff(tile, extent, md.crs, Tags.empty, GeoTiffOptions.DEFAULT)
            //geotiff.write(s"/tmp/${country.code}-masked.tif")

            // OutputPyramid.saveLayer(job.forgottenLayer, histogram, new URI("s3://un-sdg/catalog/roads"), country.code)

            job.grumpMaskRdd.unpersist()
            job.forgottenLayer.unpersist()

            spark.sparkContext.clearJobGroup()
            (country, summary)
          })
          Await.result(future, scala.concurrent.duration.Duration.Inf).toMap
        }

        result.foreach({ case (c, s) => println(c.code + " " + s.report) })

        val collection = JsonFeatureCollection()
        result.foreach { case (country, summary) =>
            val f = Feature(country.boundary, OutputProperties(country, summary).asJson)
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