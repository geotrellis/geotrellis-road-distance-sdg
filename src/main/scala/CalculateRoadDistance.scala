package geotrellis.sdg

/*
import geotrellis.raster._
import geotrellis.raster.rasterize._
import geotrellis.raster.histogram._
import geotrellis.raster.io._
import geotrellis.vector._
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
//import geotrellis.shapefile._

import org.locationtech.geomesa.spark.jts._

import osmesa.GenerateVT
import osmesa.common.ProcessOSM

import com.vividsolutions.jts.geom.{Geometry => JTSGeometry}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql._
import org.apache.spark.sql.functions.udf

import spray.json._
import DefaultJsonProtocol._


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

  def produceTiledRDD(implicit sc: SparkContext): TileLayerRDD[SpatialKey] = {
    val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/world-pop.tif")

    val md: TileLayerMetadata[SpatialKey] = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme())._2
    val mapTransform = md.layout.mapTransform

    rdd.tileToLayout(md)
  }

  def produceOSMRoadDF(implicit ss: SparkSession): DataFrame = {
    val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/south-america.orc"))
    //val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/djibouti.orc"))

    val osmRoadData =
      osmData
        .select("id", "_type", "geom", "tags")
        .withColumn("roadType", osmData("tags").getField("highway"))
        .withColumn("surfaceType", osmData("tags").getField("surface"))

    val osmRoads =
      osmRoadData
          .where(
          osmRoadData("geom").isNotNull &&
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

    osmRoads.withColumn("bufferedGeom", bufferGeomUDF(osmRoads.col("geom")))
  }

  def produceGeometryCollection: Seq[MultiPolygonFeature[Map[String, String]]] = {
    val keysOfInterest = List("adm0_a3", "continent", "name")

    val countries: Seq[MultiPolygonFeature[Map[String, Object]]] =
      ShapeFileReader.readMultiPolygonFeatures("/tmp/countries/ne_50m_admin_0_countries.shp")

    val countriesWithStrings: Seq[MultiPolygonFeature[Map[String, String]]] =
      countries.map { country =>
        val updattedKeys: Map[String, Object] =
          country.data.map { case (k, v) => (k.toLowerCase.replace(" ", "_"), v) }

        val filteredData = updattedKeys.filterKeys { key => keysOfInterest.contains(key) }

        val updattedValues: Map[String, String] =
          filteredData.mapValues { _.asInstanceOf[String].toLowerCase.replace(" ", "_") }

        country.copy(data = updattedValues)
      }

    val countriesOfInterest = List("somalia", "ethiopia", "eritrea", "djibouti")

    //countriesWithStrings.filter { country => countriesOfInterest.contains(country.data.get("name").get) }

    //countriesWithStrings.filter { country => country.data.get("continent").get == "africa" }
    countriesWithStrings.filter { country => country.data.get("name").get == "albania" }
  }

  def main(args: Array[String]): Unit = {
    System.setSecurityManager(null)

    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Road SDG")
        .set("spark.driver.memory", "1G")
        .set("spark.executor.memory", "1G")
        .set("spark.driver.maxResultSize", "3G")
        .set("spark.executor.maxResultSize", "3G")
        .set("spark.dynamicAllocation.enabled", "true")
        .set("spark.shuffle.service.enabled", "true")
        .set("spark.shuffle.compress", "true")
        .set("spark.shuffle.spill.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.driver.memoryOverhead", "1G")
        .set("spark.executor.memoryOverhead", "1G")
        .set("spark.executor.extraJavaOptions", "-XX:+UseParallelGC")
        .set("spark.serializer", classOf[KryoSerializer].getName)
        .set("spark.kryo.registrator", classOf[KryoRegistrator].getName)

    implicit val ss = SparkSession.builder.config(conf).enableHiveSupport.getOrCreate
    implicit val sc = ss.sparkContext

    ss.withJTS

    import ss.implicits._

    try {
      val countries: Seq[MultiPolygonFeature[Map[String, String]]] = produceGeometryCollection

      //Reading in and formatting the OSM data
      val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/albania.orc"))

      val osmRoadData =
        osmData
          .select("id", "_type", "geom", "tags")
          .withColumn("roadType", osmData("tags").getField("highway"))
          .withColumn("surfaceType", osmData("tags").getField("surface"))

          val osmRoads =
            osmRoadData
              .where(
                osmRoadData("geom").isNotNull &&
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

      val bufferedOSMData = osmRoads.withColumn("bufferedGeom", bufferGeomUDF(osmRoads.col("geom")))

      // Reading in and formatting the World Population data
      val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/world-pop.tif")

      val md: TileLayerMetadata[SpatialKey] = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme())._2
      val mapTransform = md.layout.mapTransform

      val tiledLayer: TileLayerRDD[SpatialKey] = rdd.tileToLayout(md).cache()

      val populationPerCountry: Seq[MultiPolygonFeature[Map[String, String]]] = {
        val calculatePop = (tile: Tile) => {
          var acc: Double = 0.0
          tile.foreachDouble { (d: Double) => if (!isNoData(d)) acc += d }
          acc
        }

        countries.map { country =>
          val maskedLayer = tiledLayer.mask(country.geom)
          val totalPop = maskedLayer.values.map { calculatePop }.reduce { _ + _ }

          country.copy(data = country.data + ("totalPopulation" -> totalPop.toString))
        }
      }

      val populationPerCountryGeoJson: String = populationPerCountry.toGeoJson

      val popPerCountryMap: Map[String, String] =
        populationPerCountry.map { country =>
          val population = country.data.get("totalPopulation").get
          val name = country.data.get("name").get

          Map((name, population))
        }.reduce { _ ++ _ }

      val getCountryName: (JTSGeometry) => String =
        (geom: JTSGeometry) => {
          populationPerCountry.map { country =>
            if (country.geom.intersects(geom))
              country.data("name")
            else
              ""
          }.toSet.reduce { _ + _ }
        }

      val getCountryNameUDF = udf(getCountryName)

      val countryOSMData =
        bufferedOSMData.withColumn("country", getCountryNameUDF(osmData("geom")))

      val getCountryPopulation: (JTSGeometry) => Double =
        (geom: JTSGeometry) => {
          populationPerCountry.map { country =>
            if (country.geom.intersects(geom))
              country.data("totalPopulation").toDouble
            else
              0.0
          }.reduce { _ + _ }
        }

      val getCountryPopulationUDF = udf(getCountryPopulation)

      val populationOSMData =
        countryOSMData.withColumn("totalPopulation", getCountryPopulationUDF(countryOSMData("geom"))).persist(StorageLevel.MEMORY_AND_DISK)

      //populationOSMData.write.format("orc").save("/tmp/south-american-country-and-road-data.orc")

      populationOSMData.unpersist()
      bufferedOSMData.unpersist()
      tiledLayer.unpersist()

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

          }.persist(StorageLevel.MEMORY_AND_DISK)

      osmData.unpersist()
      //populationOSMData.unpersist()

      val partitioner = SpacePartitioner[SpatialKey](md.bounds)

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

      for (z <- 4 to 14) {
        val layout = scheme.levelForZoom(z).layout

        val keyedFeaturesRDD: RDD[(SpatialKey, (SpatialKey, GenerateVT.VTF[Geometry]))] =
          GenerateVT.keyToLayout(featuresRDD, layout)

        val vectorTilesRDD: RDD[(SpatialKey, VectorTile)] =
          GenerateVT.makeVectorTiles(keyedFeaturesRDD, layout, "albania-roads")
          //GenerateVT.makeVectorTiles(keyedFeaturesRDD, layout, "djibouti-roads")

        //GenerateVT.save(vectorTilesRDD, z, "sourth-american-roads", "sdg/sourth-america/vectortiles")
        GenerateVT.saveHadoop(vectorTilesRDD, z, "file:///tmp/sdg-output/albania-road-vectortiles")
      }

      maskedRDD.unpersist()

      val getPop: (JTSGeometry) => Double =
        (geom: JTSGeometry) => {
          val maskedRDD = tiledLayer.mask(geom.asInstanceOf[Polygon])

          maskedRDD.values.map { calculatePop }.reduce { _ + _ }
        }

      val getPopUDF = udf(getPop)

      val popDF = countriesDF.withColumn("totalPopulation", getPopUDF(countriesDF.col("geometry")))

      //val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/djibouti.orc"))
      //val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/car.orc"))
      val osmData = ProcessOSM.constructGeometries(ss.read.orc("/tmp/horn-of-africa.orc"))

      val osmRoadData =
        osmData
          .select("id", "_type", "geom", "tags")
          .withColumn("roadType", osmData("tags").getField("highway"))
          .withColumn("surfaceType", osmData("tags").getField("surface"))

      val osmRoads =
        osmRoadData
            .where(
            osmRoadData("_type") === 2 &&
            !osmRoadData("roadType").isin(badRoads:_*)
          )

      //val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/DJI15adjv4.tif")
      //val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/car.tif")
      val rdd: RDD[(ProjectedExtent, Tile)] = HadoopGeoTiffRDD.spatial("/tmp/world-pop.tif")

      val md: TileLayerMetadata[SpatialKey] = rdd.collectMetadata[SpatialKey](FloatingLayoutScheme())._2
      val mapTransform = md.layout.mapTransform

      val bufferGeom: (JTSGeometry) => JTSGeometry =
        (jtsGeom: JTSGeometry) => {
          val center = jtsGeom.getCentroid()
          val x = center.getX()
          val y = center.getY()

          val localCRS = UTM.getZoneCrs(x, y)

          val transform = Transform(md.crs, localCRS)
          val backTransform = Transform(localCRS, md.crs)

          val gtGeom = WKT.read(jtsGeom.toText)

          val reprojected = Reproject(gtGeom, transform)
          val buffered = reprojected.jtsGeom.buffer(2.0)

          Reproject(buffered, backTransform).jtsGeom
        }

      val bufferGeomUDF = udf(bufferGeom)

      val osmBufferedRoads =
        osmRoads.withColumn("bufferedGeom", bufferGeomUDF(osmRoads.col("geom")))

      val geomRDD: RDD[VTF[Geometry]] =
        osmBufferedRoads
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
                "originalGeom" -> VString(jtsGeom.toText)
              )

            Feature(Geometry(bufferedJTSGeom), metadata)

          }.persist(StorageLevel.MEMORY_AND_DISK)

      val partitioner = SpacePartitioner[SpatialKey](md.bounds)

      val tiledLayer: TileLayerRDD[SpatialKey] = rdd.tileToLayout(md)

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

            val updatedFeatureData: Iterable[GenerateVT.VTF[Geometry]] =
              features.map { feature =>
                val geomPop = caculatePop(v.mask(tileExtent, feature.geom, options))
                val updatedData =
                  feature.data ++: Map("population" -> VDouble(geomPop))

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
        }, preservesPartitioning = true).persist()//persist(StorageLevel.MEMORY_AND_DISK)

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
          GenerateVT.makeVectorTiles(keyedFeaturesRDD, layout, "ha-roads")
          //GenerateVT.makeVectorTiles(keyedFeaturesRDD, layout, "car-roads")

        //GenerateVT.save(vectorTilesRDD, z, "geotrellis-test", "sdg/djibouti/line-vectortiles")
        //GenerateVT.saveHadoop(vectorTilesRDD, z, "file:///tmp/sdg-output/djibouti-road-vectortiles")
        //GenerateVT.saveHadoop(vectorTilesRDD, z, "file:///tmp/sdg-output/car-road-vectortiles")
        GenerateVT.saveHadoop(vectorTilesRDD, z, "file:///tmp/sdg-output/ha-road-vectortiles")
      }

      maskedRDD.unpersist()
    } finally {
      ss.stop
    }
  }
}
*/
