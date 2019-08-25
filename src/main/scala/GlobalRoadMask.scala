package geotrellis.sdg

import geotrellis.spark.store.kryo._

import org.locationtech.geomesa.spark.jts._

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._

import cats.implicits._
import com.monovore.decline._


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
          .set("spark.executor.memory", "8g")
          .set("spark.driver.memory", "8g")
          .set("spark.default.parallelism", partitionNum.toString)

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      val sqlContext = ss.sqlContext

      ss.withJTS

      val testCountries =
        Array(
          ("nicaragua", "nic"),
          ("honduras", "hnd"),
          ("el savador", "slv")
        )

      try {
        val osmRoads: Array[DataFrame] =
          testCountries.map { case (countryName, countryCode) =>

            MbTilesDownloader.download(countryName)

            MbTilesReader.readAndFormat(sqlContext, s"/tmp/${countryName}.mbtiles", countryCode)
          }

        val globalOSMRoads: DataFrame = osmRoads.reduce { _ union _ }

        globalOSMRoads
          .repartition(partitionNum, globalOSMRoads.col("tile_column"), globalOSMRoads.col("tile_row"))
          .write
          //.partitionBy(numPartitions, osmRoads.col("tile_column"), osmRoads.col("tile_row"))
          .format("orc")
          .save(output)

      } finally {
        ss.sparkContext.stop
      }
    }
  }
)
