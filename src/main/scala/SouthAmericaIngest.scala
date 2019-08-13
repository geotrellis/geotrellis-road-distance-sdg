package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.raster.histogram._
import geotrellis.raster.io._
import geotrellis.vector._
import geotrellis.vector.simplify._
import geotrellis.vector.io._
import geotrellis.vector.reproject._
import geotrellis.vector.io.wkt._
import geotrellis.vector.io.json._
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
import geotrellis.spark.io.s3.S3Client
//import geotrellis.shapefile._

import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.udf.GeometricConstructorFunctions
import osmesa.GenerateVT
import osmesa.common.ProcessOSM

import com.vividsolutions.jts.geom.{Geometry => JTSGeometry, LineString}

import org.apache.spark.{SparkConf, SparkContext, HashPartitioner}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.jts.GeometryUDT

import spray.json._
import DefaultJsonProtocol._

import scala.collection.JavaConverters._


object SouthAmericaIngest {
/*
  type VTF[G <: Geometry] = Feature[G, Map[String, VString]]

  val southAmericaExtent = Extent(-98.96484375, -57.89149735271031, -28.652343749999996, 15.623036831528264)

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

    val targetFileName = "south-america"
    val numPartitions = 15000

    val conf =
      new SparkConf()
        .setIfMissing("spark.master", "local[*]")
        //.setMaster("local[*]")
        .setAppName("Road SDG")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)
        .set("spark.sql.shuffle.partitions", numPartitions.toString)
        .set("spark.sql.broadcastTimeout", "36000")
        .set("spark.default.parallelism", numPartitions.toString)
        .set("spark.executor.memory", "8g")
        .set("spark.driver.memory", "8g")
        .set("spark.yarn.am.memory", "8g")
        .set("spark.yarn.am.memoryOverhead", "8g")
        .set("spark.driver.memoryOverhead", "8g")
        .set("spark.executor.memoryOverhead", "6g")
        .set("spark.network.timeout", "600")
        .set("spark.executor.heartbeatInterval", "100")
        //.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC -XX:-ResizePLAB -XX:InitiatingHeapOccupancyPercent=35")

    implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    implicit val sc = ss.sparkContext

    ss.withJTS

    try {

      val countries: Seq[MultiPolygonFeature[Map[String, String]]] =
        GeoJsonReader.readFromS3("un-sdg", "south-america/south-america-country-populations-epsg4326.geojson")
      b

      val osmData = ProcessOSM.constructGeometries(ss.read.orc(s"s3://un-sdg/south-america/${targetFileName}.orc"))

      val isValid: (JTSGeometry) => Boolean = (jtsGeom: JTSGeometry) => jtsGeom.isValid()

      val isValidUDF = udf(isValid)

      val osmRoadData =
        osmData
          .select("id", "_type", "geom", "tags")
          .withColumn("roadType", osmData("tags").getField("highway"))
          .withColumn("surfaceType", osmData("tags").getField("surface"))

      val osmRoads =
        osmRoadData
          .where(
            (osmRoadData("geom").isNotNull && isValidUDF(osmRoadData("geom"))) &&
            osmRoadData("_type") === 2 &&
            !osmRoadData("roadType").isin(badRoads:_*)
          )

      val bufferGeom: (JTSGeometry) => JTSGeometry =
        (jtsGeom: JTSGeometry) => {
          val center = jtsGeom.getCentroid()
          val x = center.getX()
          val y = center.getY()

          val localCRS = UTM.getZoneCrs(x, y)

          val transform = Transform(LatLng, localCRS)
          val backTransform = Transform(localCRS, LatLng)

          val gtGeom = WKT.read(jtsGeom.toText)

          val reprojected = Reproject(gtGeom, transform)
          val buffered = reprojected.jtsGeom.buffer(2.0)

          Reproject(buffered, backTransform).jtsGeom
        }

      val bufferGeomUDF = udf(bufferGeom)

      val bufferedOSMData =
        osmRoads
          .withColumn("bufferedGeom", bufferGeomUDF(osmRoads.col("geom")))

      // Reading in and formatting the World Population data
      val tiledLayer: TileLayerRDD[SpatialKey] = {
        val layerId = LayerId("world-pop-epsg4326", 0)
        val attributeStore = AttributeStore("s3://un-sdg")

        val layerMetadata = attributeStore.readMetadata[TileLayerMetadata[SpatialKey]](layerId)
        //val layerQuery = new LayerQuery[SpatialKey, TileLayerMetadata[SpatialKey]].where(Intersects(southAmericaExtent))

        val layerReader = LayerReader("s3://un-sdg")

        //layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId, layerQuery, numPartitions)
        layerReader.read[SpatialKey, Tile, TileLayerMetadata[SpatialKey]](layerId, numPartitions)
      }//.cache()

      val md = tiledLayer.metadata
      val mapTransform = md.mapTransform

      val countryInfoMap: Map[MultiPolygon, (String, String)] =
        countries.map { country =>
          val population = country.data.get("totalPopulation").get
          val name = country.data.get("name").get

          Map((country.geom -> (name, population)))
        }.reduce { _ ++ _ }

      val getCountryName: (JTSGeometry) => String =
        (geom: JTSGeometry) => {
          countryInfoMap.map { case (country, (name, _)) =>
            if (country.intersects(geom))
              name
            else
              ""
          }.toSet.reduce { _ + _ }
        }

      val getCountryNameUDF = udf(getCountryName)

      val countryOSMData =
        bufferedOSMData.withColumn("country", getCountryNameUDF(osmData("geom")))

      val getCountryPopulation: (JTSGeometry) => Double =
        (geom: JTSGeometry) => {
          countryInfoMap.map { case (country, (_, pop)) =>
            if (country.intersects(geom))
              pop.toDouble
            else
              0.0
          }.reduce { _ + _ }
        }

      val getCountryPopulationUDF = udf(getCountryPopulation)

      val populationOSMData =
        countryOSMData
          .withColumn("totalPopulation", getCountryPopulationUDF(countryOSMData("geom")))

      val geomRDD: RDD[VTF[Geometry]] =
        populationOSMData
          .rdd
          .map { geomRow =>
            val jtsGeom = geomRow.getAs[JTSGeometry]("geom")

            val bufferedJTSGeom = geomRow.getAs[JTSGeometry]("bufferedGeom")

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
                "surfaceType" -> VString(surfaceType),
                "country" -> VString(geomRow.getAs[String]("country")),
                "countryPopulation" -> VString(geomRow.getAs[Double]("totalPopulation").toString),
                "originalGeom" -> VString(jtsGeom.toText)
              )

            Feature(Geometry(bufferedJTSGeom), metadata)

          }//.persist(StorageLevel.MEMORY_AND_DISK)

      //val partitioner = SpacePartitioner[SpatialKey](md.bounds)
      //val partitioner = SpatialPartitioner(numPartitions)
      val partitioner = new HashPartitioner(numPartitions)

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

      val clippedGeoms: RDD[(SpatialKey, VTF[Geometry])] = geomRDD.clipToGrid(md.layout)

      val groupedClippedGeoms: RDD[(SpatialKey, Iterable[VTF[Geometry]])] = clippedGeoms.groupByKey(partitioner)

      val joinedRDD: RDD[(SpatialKey, (Tile, Iterable[VTF[Geometry]]))] =
        tiledLayer.join(groupedClippedGeoms, partitioner)//.persist(StorageLevel.MEMORY_AND_DISK)

      //tiledLayer.unpersist()

      //geomRDD.unpersist()

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

            val updatedFeatureData: Iterable[GenerateVT.VTF[Geometry]] =
              features.map { feature =>
                val geomPop = caculatePop(v.mask(tileExtent, feature.geom, options))
                val totalPop = feature.data("countryPopulation").value.toDouble

                val percent = geomPop / totalPop

                val updatedData =
                  feature.data ++: Map("roadPopulation" -> VDouble(geomPop), "roadPopulationPercentage" -> VDouble(percent))

                feature.copy(data = updatedData)
              }

            val maskedTile = v.mask(tileExtent, updatedFeatureData.map { _.geom }, options)

            val gtGeomFeatures =
              updatedFeatureData.map { feature =>
                val wkt = feature.data.get("originalGeom").get.asInstanceOf[VString].value
                val gtGeom = WKT.read(wkt)

                val updatedData = feature.data - "originalGeom"

                feature.copy(geom = gtGeom, data = updatedData)
              }

            (k, (maskedTile, gtGeomFeatures))
          }
        }, preservesPartitioning = true)///.persist(StorageLevel.MEMORY_AND_DISK)

      val schema =
        new StructType()
          .add(StructField("__id", StringType, nullable = false))
          .add(StructField("roadType", StringType, nullable = false))
          .add(StructField("surfaceType", StringType, nullable = false))
          .add(StructField("country", StringType, nullable = false))
          .add(StructField("countryPopulation", DoubleType, nullable = false))
          .add(StructField("roadPopulation", DoubleType, nullable = false))
          .add(StructField("roadPopulationPercentage", DoubleType, nullable = false))
          .add(StructField("geom", GeometryUDT, nullable = false))

      val rowRDD: RDD[Row] =
        updatedRDD.flatMap { case (_, (_, features)) =>
          features.map { feature =>
            val id: String = feature.data.get("__id").get.asInstanceOf[VString].value
            val country: String = feature.data.get("country").get.asInstanceOf[VString].value
            val roadType: String = feature.data.get("roadType").get.asInstanceOf[VString].value
            val surfaceType: String = feature.data.get("surfaceType").get.asInstanceOf[VString].value
            val countryPopulation: Double = feature.data.get("countryPopulation").get.asInstanceOf[VString].value.toDouble
            val roadPopulation: Double = feature.data.get("roadPopulation").get.asInstanceOf[VDouble].value
            val roadPopulationPercentage: Double = feature.data.get("roadPopulationPercentage").get.asInstanceOf[VDouble].value

            Row(id, roadType, surfaceType, country, countryPopulation, roadPopulation, roadPopulationPercentage, feature.geom.jtsGeom)
          }
        }//.persist(StorageLevel.MEMORY_AND_DISK)

      val updatedDF: DataFrame = ss.createDataFrame(rowRDD, schema)

      updatedDF.write.format("orc").save("s3a://un-sdg/south-america/south-america-road-and-population-data-2015-epsg4326.orc")

      //rowRDD.unpersist()

      //joinedRDD.unpersist()

      val schema =
        new StructType()
          .add(StructField("__id", StringType, nullable = false))
          .add(StructField("roadType", StringType, nullable = false))
          .add(StructField("surfaceType", StringType, nullable = false))
          .add(StructField("country", StringType, nullable = false))
          .add(StructField("countryPopulation", DoubleType, nullable = false))
          .add(StructField("roadPopulation", DoubleType, nullable = false))
          .add(StructField("roadPopulationPercentage", DoubleType, nullable = false))
          .add(StructField("geom", GeometryUDT, nullable = false))

      val partitionStrategy = new HashPartitioner(numPartitions)

      val osmData =
        ss
          .read
          .schema(schema)
          .orc(s"s3://un-sdg/south-america/south-america-road-and-population-data-2015-epsg4326.orc")
          //.persist(StorageLevel.MEMORY_AND_DISK)

      //val roads = List("motorway", "trunk", "primary", "secondary")
      val roads = List("motorway", "trunk")

      val osmRoads =
        osmData
          .where(osmData("roadType").isin(roads:_*))

      val geomRDD: RDD[GenerateVT.VTF[Geometry]] =
        //osmData
        osmRoads
          .rdd
          .map { geomRow =>
            val jtsGeom  = geomRow.getAs[JTSGeometry]("geom")
            val geom = Geometry(jtsGeom)//.simplify(3.0)
            val reprojectedGeom = geom.reproject(LatLng, WebMercator)

            val metadata =
              Map(
                "__id" -> VString(geomRow.getAs[String]("__id")),
                "roadType" -> VString(geomRow.getAs[String]("roadType")),
                "surfaceType" -> VString(geomRow.getAs[String]("surfaceType")),
                "country" -> VString(geomRow.getAs[String]("country")),
                "countryPopulation" -> VString(geomRow.getAs[Double]("countryPopulation").toString),
                "roadPopulation" -> VString(geomRow.getAs[Double]("roadPopulation").toString),
                "roadPopulationPercentage" -> VString(geomRow.getAs[Double]("roadPopulationPercentage").toString)
              )

            Feature(reprojectedGeom, metadata)

          }//.persist(StorageLevel.MEMORY_AND_DISK)

      //osmData.unpersist()

      val featuresRDD: RDD[GenerateVT.VTF[Geometry]] =
        updatedRDD.mapValues { case (_, features) =>
          features.map { feature =>
            val reprojected = feature.geom.reproject(LatLng, WebMercator)

            feature.copy(geom = reprojected)
          }
        }.values.flatMap { f => f }

      val targetZoom = 6
      val scheme = ZoomedLayoutScheme(WebMercator)
      val targetLayout = scheme.levelForZoom(targetZoom).layout

      //val keyedFeaturesRDD: RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[Geometry]))] =
        //GenerateVT
          //.keyToLayout(geomRDD, targetLayout)
          //.persist(StorageLevel.MEMORY_AND_DISK)

      //updatedRDD.unpersist()

      def produceRDDs(
        rdd: RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[Geometry]))],
        zoom: Int
      ): List[(Int, RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[Geometry]))])] =
        if (zoom == 6) {
          val vtRDD = GenerateVT.upLevel(rdd)//.persist()

          List((zoom, rdd)) ++ produceRDDs(vtRDD, zoom - 1)
        } else if (zoom == 3)
          List((zoom, rdd))
        else
          List[(Int, RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[Geometry]))])]()

      val rdds = produceRDDs(keyedFeaturesRDD, targetZoom)

      rdds.foreach { case (z, vt) =>
        val layout = scheme.levelForZoom(z).layout

        val vectorTilesRDD: RDD[(SpatialKey, VectorTile)] =
          GenerateVT.makeVectorTiles(vt, layout, s"${targetFileName}-roads")

        GenerateVT.save(vectorTilesRDD, z, "un-sdg", s"south-america/${targetFileName}-vectortiles")

        //vt.unpersist()
      }

      //osmData.unpersist()

      for (z <- 6 to 3 by -1) {
        val layout = scheme.levelForZoom(z).layout

        val keyedFeaturesRDD: RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[Geometry]))] =
          GenerateVT.keyToLayout(geomRDD, layout)

        val vectorTilesRDD: RDD[(SpatialKey, VectorTile)] =
          GenerateVT.makeVectorTiles(keyedFeaturesRDD, layout, s"${targetFileName}-roads")//.persist()

        GenerateVT.save(vectorTilesRDD, targetZoom, "un-sdg", s"south-america/${targetFileName}-vectortiles")

        //vectorTilesRDD.unpersist()
      }

      //keyedFeaturesRDD.unpersist()

    } finally {
      ss.stop
    }
  }

  def getNumPartitions[K: SpatialComponent, M](
    layerQuery: LayerQuery[K, TileLayerMetadata[K]],
    layerMetadata: TileLayerMetadata[K]
  ): Int = {
    val tileBytes = (layerMetadata.cellType.bytes
        * layerMetadata.layout.tileLayout.tileCols
        * layerMetadata.layout.tileLayout.tileRows)
    // Aim for ~16MB per partition
    val tilesPerPartition = (1 << 24) / tileBytes
    // TODO: consider temporal dimension size as well
    val expectedTileCounts: Seq[Long] = layerQuery(layerMetadata).map(_.toGridBounds.size)
    try {
      math.max(1, (expectedTileCounts.reduce( _ + _) / tilesPerPartition).toInt)
    } catch {
      case e: java.lang.UnsupportedOperationException => 1
    }
  }
*/
}
