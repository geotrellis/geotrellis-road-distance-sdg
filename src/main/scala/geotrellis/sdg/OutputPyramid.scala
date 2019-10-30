package geotrellis.sdg

import java.net.URI

import com.uber.h3core.H3Core
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.layer._
import geotrellis.proj4.WebMercator
import geotrellis.raster.resample.Sum
import geotrellis.spark.store.LayerWriter
import geotrellis.spark.store.hadoop.SaveToHadoop
import geotrellis.spark.store.s3.SaveToS3
import geotrellis.store.{AttributeStore, LayerId}
import geotrellis.store.index.ZCurveKeyIndexMethod
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import software.amazon.awssdk.services.s3.model.ObjectCannedACL
import vectorpipe.VectorPipe


object OutputPyramid {
  /**
    * Generate an image pyramid from the passed TileLayerRDD using the provided ColorRamp
    * to construct a ColorMap with linear breaks
    *
    * @param layer
    * @param colorMap
    * @param outputPath
    * @return
    */
  def savePng(
    layer: TileLayerRDD[SpatialKey],
    colorMap: ColorMap,
    outputPath: String
  ): Unit = {
    implicit val sc: SparkContext = layer.sparkContext

    val (baseZoom, baseLayer) = layer.reproject(ZoomedLayoutScheme(WebMercator, 256))
    val pyramid = Pyramid.fromLayerRDD(baseLayer, Some(baseZoom), Some(0))

    pyramid.levels.foreach { case (zoom, tileRdd) =>
      val imageRdd: RDD[(SpatialKey, Array[Byte])] =
        tileRdd.mapValues(_.renderPng(colorMap).bytes)

      val keyToPath = { k: SpatialKey => s"${outputPath}/${zoom}/${k.col}/${k.row}.png" }
      if (outputPath.startsWith("s3")) {
        SaveToS3(imageRdd, keyToPath, { request =>
          request.toBuilder.acl(ObjectCannedACL.PUBLIC_READ).build()
        })
      } else {
        SaveToHadoop(imageRdd, keyToPath)
      }
    }
  }

  def saveLayer(
    layer: TileLayerRDD[SpatialKey],
    histogram: StreamingHistogram,
    catalog: URI,
    layerName: String
  ): Unit = {
    val pyramid = Pyramid.fromLayerRDD(layer, resampleMethod = Sum, endZoom = Some(1))
    val store = AttributeStore(catalog)
    val writer = LayerWriter(store, catalog)
    store.write(LayerId(layerName, 0), "histogram", histogram)
    for (z <- pyramid.maxZoom to pyramid.minZoom by -1) {
      val id = LayerId(layerName, z)
      if (store.layerExists(id)) writer.update(id, pyramid.levels(z))
      else writer.write(id, pyramid.levels(z), ZCurveKeyIndexMethod)
    }
  }

  /**
    * Generates a vector tile layer of aggregated hex bins representing the
    * underlying population in the WorldPop 100m raster.
    *
    * Specifically coded to only work with PopulationNearRoadsJob.forgottenLayer ATM.
    *
    * Utilizes the Uber H3 hex library to index and generate hex geometries.
    *
    * @param layer
    * @param outputUri S3 or local file URI to write the layer to
    * @param spark
    */
  def forgottenLayerHexVectorTiles(layer: TileLayerRDD[SpatialKey], outputUri: URI)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val maxZoom = 10
    val minZoom = 6

    val layout = layer.metadata.layout
    val gridPointsRdd: RDD[(String, Double)] = layer.flatMap {
      case (key: SpatialKey, tile: Tile) => {
        val h3: H3Core = H3Core.newInstance
        val tileExtent = key.extent(layout)
        val re = RasterExtent(tileExtent, tile)
        for {
          col <- Iterator.range(0, tile.cols)
          row <- Iterator.range(0, tile.rows)
          v = tile.getDouble(col, row)
          if isData(v)
        } yield {
          val (lon, lat) = re.gridToMap(col, row)
          // Higher number makes larger hexagons
          // Zero means that our starting maxZoom == h3 hex "resolution"
          val hexZoomOffset = 2
          val h3Index = h3.geoToH3Address(lat, lon, maxZoom - hexZoomOffset)
          (h3Index, v)
        }
      }
    }

    val pipeline = ForgottenPopPipeline(
      "geom",
      outputUri,
      maxZoom
    )
    val vpOptions = VectorPipe.Options(
      maxZoom = maxZoom,
      minZoom = Some(minZoom),
      srcCRS = WebMercator,
      destCRS = None,
      useCaching = false,
      orderAreas = false
    )

    val gridPointsDf = gridPointsRdd
      .toDF("h3Index", "pop")
      .withColumn("geom", pipeline.geomUdf(col("h3Index")))
    VectorPipe(gridPointsDf, pipeline, vpOptions)
  }
}
