package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.kryo._

import osmesa.common.ProcessOSM

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._


object CalculateRoadDistance {
  val badSurfaces =
    List(
      "compacted",
      "fine_gravel",
      "gravel",
      "pebblestone",
      "grass_paver",
      "grass",
      "dirt",
      "earth",
      "mud",
      "sand",
      "ground"
    )

  def main(args: Array[String]): Unit = {
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Road Distance SDG")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

    implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    implicit val sc = ss.sparkContext

    val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/djibouti.orc"))

    val osmRoadData =
      osmData
        .select("_type", "geom", "tags")
        .withColumn("roadType", osmData("tags").getField("highway"))
        .withColumn("surfaceType", osmData("tags").getField("surface"))

    val osmRoads =
      osmRoadData
          .where(
          osmRoadData("_type") =!= 1 &&
          osmRoadData("highway").isNotNull &&
          (osmRoadData("surfaceType").isNull || badSurfaces.map { !osmRoadData("surfaceType").contains(_) }.reduce(_ && _))
        ).select(
          "geom"
        )

    val geomRDD: RDD[Geometry] = ???

    val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/DJI15adjv4.tif")

    val md: TileLayerMetadata[SpatialKey] = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme())._2
    val mapTransform = md.layout.mapTransform

    val partitioner = SpacePartitioner[SpatialKey](md.bounds)

    val tiledLayer: TileLayerRDD[SpatialKey] = rdd.tileToLayout(md)

    val clippedGeoms: RDD[(SpatialKey, Geometry)] = geomRDD.clipToGrid(md.layout)
    val groupedClippedGeoms: RDD[(SpatialKey, Iterable[Geometry])] = clippedGeoms.groupByKey(partitioner)

    val joinedRDD: RDD[(SpatialKey, (Tile, Iterable[Geometry]))] =
      tiledLayer.join(groupedClippedGeoms, partitioner)

    val options: Rasterizer.Options = Rasterizer.Options(true, PixelIsArea)

    val maskedRDD: RDD[(SpatialKey, Tile)] =
      joinedRDD.mapPartitions({ partition =>
        partition.map { case (k, (v, geoms)) =>
          (k, v.mask(mapTransform(k), geoms, options))
        }
      }, preservesPartitioning = true
    )
  }
}
