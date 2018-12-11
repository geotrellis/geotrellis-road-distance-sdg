package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.raster.histogram._
import geotrellis.raster.io._
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.vector.io.wkt._
import geotrellis.proj4._
import geotrellis.proj4.util._
import geotrellis.spark._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.tiling._
import geotrellis.spark.partition.SpacePartitioner
import geotrellis.spark.io._
import geotrellis.spark.io.hadoop._
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.kryo._

import osmesa.common.ProcessOSM

import com.vividsolutions.jts.geom.{Geometry => JTSGeometry}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
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

  val badRoads =
    List(
      "proposed",
      "construction",
      "elevator"
    )

  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)

    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Road Distance SDG")
        .set("spark.driver.memory", "3G")
        .set("spark.executor.memory", "3G")
        .set("spark.default.parallelism", "8")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.shuffle.service.enabled", "true")
        .set("spark.shuffle.compress", "true")
        .set("spark.shuffle.spill.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.driver.maxResultSize", "3G")
        .set("spark.task.maxFailures", "33")
        .set("spark.executor.extraJavaOptions", "-XX:+UseParallelGC")

    implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    implicit val sc = ss.sparkContext

    try {
      val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/djibouti.orc"))

      val osmRoadData =
        osmData
          .select("_type", "geom", "tags")
          .withColumn("roadType", osmData("tags").getField("highway"))
          .withColumn("surfaceType", osmData("tags").getField("surface"))

      val osmRoads =
        osmRoadData
            .where(
            osmRoadData("_type") === 2 &&
            badRoads.map { !osmRoadData("roadType").contains(_) }.reduce(_ && _) &&
            (osmRoadData("surfaceType").isNull || badSurfaces.map { !osmRoadData("surfaceType").contains(_) }.reduce(_ && _))
          ).select(
            "geom"
          )

    val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/DJI15adjv4.tif")

    val md: TileLayerMetadata[SpatialKey] = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme())._2
    val mapTransform = md.layout.mapTransform

    val geomRDD: RDD[Geometry] =
      osmRoads
        .rdd
        .map { geomRow =>
          val jtsGeom = geomRow.getAs[JTSGeometry]("geom")
          val center = jtsGeom.getCentroid()
          val x = center.getX()
          val y = center.getY()

          val localCRS = UTM.getZoneCrs(x, y)

          val transform = Transform(md.crs, localCRS)
          val backTransform = Transform(localCRS, md.crs)

          val gtGeom = WKT.read(jtsGeom.toText)

          val reprojected = Reproject(gtGeom, transform)
          val buffered = reprojected.jtsGeom.buffer(20)

          Reproject(buffered, backTransform)
        }.persist(StorageLevel.MEMORY_AND_DISK)

    val partitioner = SpacePartitioner[SpatialKey](md.bounds)

    val tiledLayer: TileLayerRDD[SpatialKey] = rdd.tileToLayout(md)

    val clippedGeoms: RDD[(SpatialKey, Geometry)] = geomRDD.clipToGrid(md.layout)

    val groupedClippedGeoms: RDD[(SpatialKey, Iterable[Geometry])] = clippedGeoms.groupByKey(partitioner)

    val joinedRDD: RDD[(SpatialKey, (Tile, Iterable[Geometry]))] =
      tiledLayer.join(groupedClippedGeoms, partitioner).persist(StorageLevel.MEMORY_AND_DISK)

    geomRDD.unpersist()

    val options: Rasterizer.Options = Rasterizer.Options(true, PixelIsArea)

    val maskedRDD: TileLayerRDD[SpatialKey] =
      ContextRDD(
        joinedRDD.mapPartitions({ partition =>
          partition.map { case (k, (v, geoms)) =>
            (k, v.mask(mapTransform(k), geoms, options))
          }
        }, preservesPartitioning = true),
        md
      ).persist(StorageLevel.MEMORY_AND_DISK)

    joinedRDD.unpersist()

    val targetZoom = 12
    val scheme = ZoomedLayoutScheme(WebMercator)
    val targetLayout = scheme.levelForZoom(targetZoom).layout

    val (_, reprojectedRDD) =
      maskedRDD
        .reproject(
          WebMercator,
          targetLayout
        )

    val pyramid: Stream[(Int, TileLayerRDD[SpatialKey])] =
      Pyramid.levelStream(reprojectedRDD, scheme, startZoom = targetZoom, endZoom = 0)

    val writer = LayerWriter("file:///tmp/sdg-output")

    pyramid.foreach { case (z, layer) =>
      if (z == targetZoom) {
        val store = writer.attributeStore
        val hist = layer.histogram

        store.write(LayerId("djibouti-sdg-2015-epsg3857", z), "histogram", hist)
      }

      writer.write(LayerId("djibouti-sdg-2015-epsg3857", z), layer, ZCurveKeyIndexMethod)
    }

    writer.write(LayerId("djibouti-sdg-2015-native", 0), maskedRDD, ZCurveKeyIndexMethod)

    maskedRDD.unpersist()

    } finally {
      ss.stop
    }
  }
}
