package geotrellis.sdg

import java.net.URI

import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.layer._
import geotrellis.proj4.WebMercator
import geotrellis.raster.render.ColorRamp
import geotrellis.raster.resample.Sum
import geotrellis.spark.store.LayerWriter
import geotrellis.spark.store.hadoop.SaveToHadoop
import geotrellis.spark.store.s3.SaveToS3
import geotrellis.store.{AttributeStore, LayerId}
import geotrellis.store.index.ZCurveKeyIndexMethod
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


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
        SaveToS3(imageRdd, keyToPath)
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
}
