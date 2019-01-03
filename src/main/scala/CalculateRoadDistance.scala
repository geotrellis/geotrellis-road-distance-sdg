package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.raster.histogram._
import geotrellis.raster.io._
import geotrellis.vector._
import geotrellis.vector.reproject._
import geotrellis.vector.io.wkt._
import geotrellis.vectortile._
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

import osmesa.GenerateVT
import osmesa.common.ProcessOSM

import com.vividsolutions.jts.geom.{Geometry => JTSGeometry}

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._


object CalculateRoadDistance {
  type VTF[G <: Geometry] = Feature[G, Map[String, VString]]

  val badSurfaces: List[String] =
    List(
      "compacted",
      "woodchips",
      "grass_paver",
      "grass",
      "dirt",
      "earth",
      "mud",
      "ground",
      "fine_gravel",
      "gravel",
      "gravel_turf",
      "pebblestone",
      "salt",
      "sand",
      "snow",
      "unpaved"
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

    import ss.implicits._

    try {
      //val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/djibouti.orc"))
      val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/car.orc"))

      val osmRoadData =
        osmData
          .select("id", "_type", "geom", "tags")
          .withColumn("roadType", osmData("tags").getField("highway"))
          .withColumn("surfaceType", osmData("tags").getField("surface"))

      val osmRoads =
        osmRoadData
            .where(
            osmRoadData("_type") === 2 &&
            !osmRoadData("roadType").isin(badRoads:_*)// &&
            //!osmRoadData("surfaceType").isin(badSurfaces:_*)
          )

      //val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/DJI15adjv4.tif")
      val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/car.tif")

      val md: TileLayerMetadata[SpatialKey] = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme())._2
      val mapTransform = md.layout.mapTransform

      val geomRDD: RDD[VTF[Geometry]] =
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

            val backProjected = Reproject(buffered, backTransform)

            val roadType =
              geomRow.getAs[String]("roadType") match {
                case null => "null"
                case s: String => s
              }

            val surfaceType =
              geomRow.getAs[String]("surfaceType") match {
                case null => "null"
                case s: String => s
              }

            val metadata =
              Map(
                "__id" -> VString(geomRow.getAs[Long]("id").toString),
                "roadType" -> VString(roadType),
                "surfaceType" -> VString(surfaceType)
              )

            Feature(backProjected, metadata)

          }.persist(StorageLevel.MEMORY_AND_DISK)

      val partitioner = SpacePartitioner[SpatialKey](md.bounds)

      val tiledLayer: TileLayerRDD[SpatialKey] = rdd.tileToLayout(md)

      /*
      val totalPop: Double = {
        tiledLayer
          .values
          .map { v =>
            var acc: Double = 0.0

            v.foreachDouble { (d: Double) => if (!isNoData(d)) acc += d }

            acc
          }
          .reduce { _ + _ }
      }
      */

      val clippedGeoms: RDD[(SpatialKey, VTF[Geometry])] = geomRDD.clipToGrid(md.layout)

      val groupedClippedGeoms: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = clippedGeoms.groupByKey(partitioner)

      val joinedRDD: RDD[(SpatialKey, (Tile, Iterable[VTF[Geometry]]))] =
        tiledLayer.join(groupedClippedGeoms, partitioner).persist(StorageLevel.MEMORY_AND_DISK)

      geomRDD.unpersist()

      val options: Rasterizer.Options = Rasterizer.Options(true, PixelIsArea)

      val caculatePop = (tile: Tile) => {
        var acc: Double = 0.0
        tile.foreachDouble { (d: Double) => if (!isNoData(d)) acc += d }
        acc
      }

      val updatedRDD: RDD[(SpatialKey, (Tile, Iterable[GenerateVT.VTF[Geometry]]))]=
        joinedRDD.mapPartitions({ partition =>
          partition.map { case (k, (v, features)) =>
            val tileExtent = mapTransform(k)

            val updatedFeatures: Iterable[GenerateVT.VTF[Geometry]] =
              features.map { feature =>
                val geomPop = caculatePop(v.mask(tileExtent, feature.geom, options))
                //val updatedData = feature.data ++: Map("population" -> VString(geomPop.toString))
                val updatedData = feature.data ++: Map("population" -> VDouble(geomPop))

                feature.copy(data = updatedData)
              }

            (k, (v.mask(tileExtent, updatedFeatures.map { _.geom }, options), updatedFeatures))
          }
        }, preservesPartitioning = true).persist(StorageLevel.MEMORY_AND_DISK)

      val maskedRDD: TileLayerRDD[SpatialKey] =
        ContextRDD(updatedRDD.mapValues { _._1 }, md)

      val featuresRDD: RDD[GenerateVT.VTF[Geometry]] =
        updatedRDD.mapValues { case (_, features) =>
          features.map { feature =>
            val reprojected = feature.geom.reproject(LatLng, WebMercator)

            feature.copy(geom = reprojected)
          }
        }.values.flatMap { f => f}

      joinedRDD.unpersist()
      updatedRDD.unpersist()

      val targetZoom = 14
      val scheme = ZoomedLayoutScheme(WebMercator)
      val targetLayout = scheme.levelForZoom(targetZoom).layout

      for (z <- 0 to 14) {
        val layout = scheme.levelForZoom(z).layout

        val keyedFeaturesRDD: RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[Geometry]))] =
          GenerateVT.keyToLayout(featuresRDD, layout)

        val vectorTilesRDD: RDD[(SpatialKey, VectorTile)] =
          //GenerateVT.makeVectorTiles(keyedFeaturesRDD, layout, "djibouti-roads")
          GenerateVT.makeVectorTiles(keyedFeaturesRDD, layout, "car-roads")

        //GenerateVT.save(vectorTilesRDD, z, "geotrellis-test", "sdg/djibouti/vectortiles")
        GenerateVT.saveHadoop(vectorTilesRDD, z, "file:///tmp/sdg-output/car-road-vectortiles")
      }

      /*
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

          store.write(LayerId("djibouti-sdg-all-weather-roads-2015-epsg3857", z), "histogram", hist)
        }

        writer.write(LayerId("djibouti-sdg-all-weather-roads-2015-epsg3857", z), layer, ZCurveKeyIndexMethod)
      }

      writer.write(LayerId("djibouti-sdg-2015-native", 0), maskedRDD, ZCurveKeyIndexMethod)
      */

      maskedRDD.unpersist()
    } finally {
      ss.stop
    }
  }
}
