package geotrellis.sdg

import java.io.PrintWriter
import java.net.URI

import cats.implicits._
import com.monovore.decline._
import geotrellis.layer._
import geotrellis.proj4.LatLng
import geotrellis.qatiles.RoadTags
import geotrellis.vector._
import geotrellis.vector.io.json.JsonFeatureCollection
import _root_.io.circe.syntax._
import org.apache.spark.storage.StorageLevel
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark._
import org.apache.spark.sql._
import org.locationtech.geomesa.spark.jts._

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

    val tileLayerUriPrefixOpt = Opts.option[URI](long = "tileLayerUriPrefix",
      help = "URI prefix to write vector tile layers to. Writes forgotten pop and road layers.").
      orNone

    val partitions = Opts.option[Int]("partitions",
      help = "spark.default.parallelism").
      orNone

    val debugOpt = Opts.flag("debug",
      help = "Debug mode. This will write the forgotten layer to a single GeoTiff.").
      orFalse

    // TODO: Add --playbook parameter that allows swapping countries.csv
    // TODO: Add option to save JSON without country borders

    (countryOpt, excludeOpt, outputPath, outputCatalogOpt, tileLayerUriPrefixOpt, partitions, debugOpt).mapN {
      (countriesInclude, excludeCountries, output, outputCatalog, tileLayerUriPrefix, partitionNum, debug) =>

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

        val result: List[(Country, PopulationSummary, RoadHistogram)] = {
          val future = Future.traverse(countries)( country => Future {
            spark.sparkContext.setJobGroup(country.code, country.name)

            val rasterSource = country.rasterSource
            println(s"Reading: $country: ${rasterSource.name}")
            println(s"Extent: ${country.code}: ${rasterSource.extent}")
            val layout = LayoutDefinition(rasterSource.gridExtent, 256)
            val roadHistogram = RoadHistogram.empty
            spark.sparkContext.register(roadHistogram, s"road-histogram-${country.code}")
            val job = new PopulationNearRoadsJob(country, grumpRdd, layout, LatLng, roadHistogram,
              { t => RoadTags.includedValues.contains(t.highway.getOrElse("")) })

            job.grumpMaskRdd.persist(StorageLevel.MEMORY_AND_DISK_SER)
            job.forgottenLayer.persist(StorageLevel.MEMORY_AND_DISK_SER)
            val (summary, histogram) = job.result

            if (debug) {
              PopulationNearRoadsJob
                .layerToGeoTiff(job.forgottenLayer)
                .write(s"/tmp/sdg-${country.code}-all-roads.tif")
            }

            outputCatalog.foreach { uri =>
              OutputPyramid.saveLayer(job.forgottenLayer, histogram, uri, country.code)
            }

            println(s"Histogram: ${histogram.minValue}, ${histogram.maxValue}")

            tileLayerUriPrefix match {
              case Some(tileLayerUri) => {
                OutputPyramid.savePng(
                  job.forgottenLayer,
                  SDGColorMaps.global,
                  outputPath = s"$tileLayerUri/${country.code}/forgotten-pop-global"
                )
                job.roadLayerTiles(
                  URI.create(s"$tileLayerUri/${country.code}/roads"),
                  maxZoom = 10,
                  minZoom = 6
                )
              }
              case _ => println("Skipped generating tile layers. Use --tileLayerUriPrefix to save.")
            }

            job.forgottenLayer.unpersist()
            job.grumpMaskRdd.unpersist()

            spark.sparkContext.clearJobGroup()
            (country, summary, roadHistogram)
          })
          Await.result(future, scala.concurrent.duration.Duration.Inf)
        }

        result.foreach({ case (c, s, _) => println(c.code + " " + s.report) })

        val collection = JsonFeatureCollection()
        result.foreach { case (country, summary, roadHistogram) =>
            val f = Feature(country.boundary, OutputProperties(country, summary, roadHistogram).asJson)
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
