package geotrellis.sdg

import geotrellis.vector._
import geotrellis.spark.store.kryo._

import org.locationtech.geomesa.spark.jts._

import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.jts.GeometryUDT

import java.io.PrintWriter


object GeomTestingUtility {
  def main(args: Array[String]): Unit =
    dumpGeoms()

  def dumpGeoms(): Unit = {
    System.setSecurityManager(null)

      val conf =
        new SparkConf()
          .setIfMissing("spark.master", "local[*]")
          .setAppName("Road Summary")
          .set("spark.serializer", classOf[KryoSerializer].getName)
          .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
          .set("spark.executor.memory", "8g")
          .set("spark.driver.memory", "8g")
          .set("spark.default.parallelism", "120")

      implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
      val sqlContext = ss.sqlContext

      ss.withJTS

      try {
        val inputSchema =
          new StructType()
            .add(StructField("zoom_level", IntegerType, nullable = false))
            .add(StructField("tile_column", IntegerType, nullable = false))
            .add(StructField("tile_row", IntegerType, nullable = false))
            .add(StructField("geom", GeometryUDT, nullable = false))
            .add(StructField("type", StringType, nullable = false))
            .add(StructField("roadType", StringType, nullable = false))
            .add(StructField("surfaceType", StringType, nullable = false))
            .add(StructField("bufferedGeom", GeometryUDT, nullable = false))
            .add(StructField("countryName", StringType, nullable = false))

        val osmData: DataFrame =
          ss
            .read
            .schema(inputSchema)
            .orc("/tmp/djibouti-roads.orc")

        val jsonString: String =
          GeometryCollection(osmData.rdd.map { case row => row.getAs[Geometry]("bufferedGeom") }.take(10)).toGeoJson

        new PrintWriter("/tmp/geom-dump.geojson") { write(jsonString); close }
      } finally {
        ss.sparkContext.stop
      }
  }
}
