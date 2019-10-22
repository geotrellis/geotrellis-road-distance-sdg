package geotrellis.sdg

import geotrellis.layer._
import geotrellis.proj4.LatLng
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import cats.implicits._
import com.monovore.decline._
import org.locationtech.geomesa.spark.jts._
import _root_.io.circe.syntax._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.sql._
import java.io.PrintWriter
import java.net.URI

import scala.concurrent.{Await, Future}


/**
  * Summary of population within 2km of roads
  */
object PopulationNearRoads extends CommandApp(
  name = "Population Near Roads",
  header = "Summarize population within and without 2km of OSM roads",
  main = {
    val countryOpt = Opts.options[Country](long = "country", short = "c",
      help = "ISO 3 country code to use for input (cf https://unstats.un.org/unsd/tradekb/knowledgebase/country-code)").
      withDefault(Country.all)

    val excludeOpt = Opts.options[Country](long = "exclude", short = "x",
      help = "Country code to exclude from input").
      orEmpty

    val outputPath = Opts.option[String](long = "output",
      help = "The path/uri of the summary JSON file")

    val outputCatalogOpt = Opts.option[URI](long = "catalog",
      help = "Catalog path to for saving masked WorldPop layers").
      orNone

    val outputTileLayerOpt = Opts.option[URI](long = "outputTileLayer",
      help = "The URI to which the forgotten pop vector tile layer should be saved. Must be a file or s3 URI").
      orNone

    val tilePixelScaleOpt = Opts.option[Int](
      long = "pixelScale",
      help = "Scaling factor for tile pixel size. The higher the number the smaller the pixels.")
      .withDefault(4)

    val partitions = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    // TODO: Add --playbook parameter that allows swapping countries.csv
    // TODO: Add option to save JSON without country borders

    (countryOpt, excludeOpt, outputPath, outputCatalogOpt, outputTileLayerOpt, tilePixelScaleOpt, partitions).mapN {
      (countriesInclude, excludeCountries, output, outputCatalog, outputTileLayer, tilePixelScale, partitionNum) =>

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
            println(s"Reading: $country: ${rasterSource.name}")
            val layout = LayoutDefinition(rasterSource.gridExtent, 256)

            val job = new PopulationNearRoadsJob(country, grumpRdd, layout, LatLng,
              { t => t.isPossiblyMotorRoad /* && t.isStrictlyAllWeather*/ } )

            job.grumpMaskRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
            job.forgottenLayer.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val (summary, histogram) = job.result

            PopulationNearRoadsJob.layerToGeoTiff(job.forgottenLayer).write(s"/tmp/sdg-${country.code}-all-roads.tif")

            outputCatalog.foreach { uri =>
              OutputPyramid.saveLayer(job.forgottenLayer, histogram, uri, country.code)
            }

            outputTileLayer match {
              case Some(tileLayerUri) => job.forgottenLayerTiles(tileLayerUri, tilePixelScale)
              case _ => println("Skipped generating forgotten pop vector tile layer. Use --outputTileLayer to save.")
            }

            job.forgottenLayer.unpersist()
            job.grumpMaskRdd.unpersist()

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