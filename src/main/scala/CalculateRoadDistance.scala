package geotrellis.sdg

import geotrellis.spark.io.kryo._

import osmesa.common.ProcessOSM

import org.apache.spark.SparkConf
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._


object CalculateRoadDistance {
  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .set("master", "local[*]")
        .setAppName("Road Distance SDG")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

    implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
  }
}
