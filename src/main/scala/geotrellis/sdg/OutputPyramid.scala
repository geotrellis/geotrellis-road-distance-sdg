package geotrellis.sdg

import java.net.URI

import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark._
import geotrellis.raster._
import geotrellis.layer._
import geotrellis.proj4.WebMercator
import geotrellis.raster.resample.Sum
import geotrellis.spark.store.LayerWriter
import geotrellis.spark.store.hadoop.SaveToHadoop
import geotrellis.store.{AttributeStore, LayerId}
import geotrellis.store.index.ZCurveKeyIndexMethod
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


object OutputPyramid {

  def savePng(
    layer: TileLayerRDD[SpatialKey],
    histogram: StreamingHistogram,
    outputPath: String
  ): Unit = {
    implicit val sc: SparkContext = layer.sparkContext

    val (baseZoom, baseLayer) = layer.reproject(ZoomedLayoutScheme(WebMercator, 256))
    val pyramid = Pyramid.fromLayerRDD(baseLayer, Some(baseZoom), Some(0))

    pyramid.levels.foreach { case (zoom, tileRdd) =>
      val colorRamp = ColorRamps.Viridis
      //val Some((min, max)) = histogram.minMaxValues()
      //val breaks = for { break <- (0 to 64)} yield min + (break * (max-min) / 64)
      val colorMap = colorRamp.toColorMap(histogram)

      val imageRdd: RDD[(SpatialKey, Array[Byte])] =
        tileRdd.mapValues(_.renderPng(colorMap).bytes)

      SaveToHadoop(imageRdd, { (k: SpatialKey) => s"${outputPath}/${zoom}/${k.col}/${k.row}.png" })
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
