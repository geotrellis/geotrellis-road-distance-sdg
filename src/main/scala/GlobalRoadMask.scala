package geotrellis.sdg

import geotrellis.spark.store.kryo._

import org.locationtech.geomesa.spark.jts._

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.hadoop.fs.{FileSystem, Path}

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
          .set("spark.default.parallelism", partitionNum.toString)
          .set("spark.driver.memoryOverhead", "3G")
          .set("spark.executor.memoryOverhead", "3G")
          .set("spark.memory.storageFraction", "0.0")
          .set("spark.driver.memory", "32G")
          .set("spark.executor.memory", "32G")

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      implicit val sqlContext = ss.sqlContext

      ss.withJTS

      try {
        //val fs = FileSystem.get(ss.sparkContext.hadoopConfiguration)
        CountryDirectory.countries.foreach { case (countryName, countryCode) =>
          MbTilesDownloader.download(countryName)
        }

        val osmRoads: Array[DataFrame] =
          //CountryDirectory.countries.filter { case (name, _) => name != "japan" }.map { case (countryName, countryCode) =>
          CountryDirectory.countries.map { case (countryName, countryCode) =>
            //MbTilesDownloader.download(countryName)

            val fileName: String = s"/tmp/${countryName}.mbtiles"
            //val filePath = new Path(fileName)

            val result: DataFrame = MbTilesReader.readAndFormat(fileName, countryCode, partitionNum)

            //fs.delete(filePath)

            result
          }

        val globalOSMRoads: DataFrame = osmRoads.reduce { _ union _ }

        globalOSMRoads
          .repartition(partitionNum, globalOSMRoads.col("tile_column"), globalOSMRoads.col("tile_row"))
          .write
          .format("orc")
          .save(output)

      } finally {
        ss.sparkContext.stop
      }
    }
  }
)
