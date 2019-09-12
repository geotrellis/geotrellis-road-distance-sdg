package geotrellis.sdg

import geotrellis.spark.store.kryo._

import org.locationtech.geomesa.spark.jts._

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._

import cats.implicits._
import com.monovore.decline._


/*
object ProduceRoadMask extends CommandApp(
  name = "Road Masking",
  header = "Poduces road geometries from OSM as an orc file",
  main = {
    val mbtilesFile = Opts.option[String]("mbtiles", help = "The path to the mbtiles file that should be read")
    val country = Opts.option[String]("country", help = "The Alpha-3 code for a country from the ISO 3166 standard")
    val outputPath = Opts.option[String]("output", help = "The path that the resulting orc fil should be written to")
    val partitions = Opts.option[Int]("partitions", help = "The number of Spark partitions to use").withDefault(120)

    (mbtilesFile, country, outputPath, partitions).mapN { (targetFile, countryCode, output, partitionNum) =>
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

      try {
        val osmRoads: DataFrame = MbTilesReader.readAndFormat(sqlContext, targetFile, countryCode).repartition(partitionNum)

        osmRoads
          .repartition(partitionNum, osmRoads.col("tile_column"), osmRoads.col("tile_row"))
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
*/
