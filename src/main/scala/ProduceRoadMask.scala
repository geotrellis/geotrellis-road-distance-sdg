package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector.{Extent, Geometry, Feature}
import geotrellis.vectortile._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.io._
import geotrellis.spark.io.cog._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.kryo._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.rasterize._
import geotrellis.spark.tiling._

import org.locationtech.geomesa.spark.jts._
//import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions

import com.vividsolutions.jts.geom.{Geometry => JTSGeometry, Polygon}

import org.apache.commons.io.IOUtils

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._


object ProduceRoadMask {
  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)

    val conf =
      new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        .setAppName("Road Mask")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
        .set("spark.executor.memory", "8g")
        .set("spark.driver.memory", "8g")
        .set("spark.default.parallelism", "120")
        //.set("spark.yarn.am.memory", "8g")
        //.set("spark.yarn.am.memoryOverhead", "8g")
        //.set("spark.driver.memoryOverhead", "8g")
        //.set("spark.executor.memoryOverhead", "6g")
        //.set("spark.network.timeout", "600")
        //.set("spark.executor.heartbeatInterval", "100")

    implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    val sqlContext = ss.sqlContext

    ss.withJTS

    try {
      val transform = MapKeyTransform(WebMercator, 4096, 4096)

      val osmRoads = GeomDataReader.readAndFormat(sqlContext, transform)

      osmRoads.write.format("orc").save("/tmp/osm-roads.orc")

    } finally {
      ss.sparkContext.stop
    }
  }
}
