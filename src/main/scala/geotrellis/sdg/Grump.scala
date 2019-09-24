package geotrellis.sdg

import geotrellis.vector._
import geotrellis.proj4._
import geotrellis.layer._
import geotrellis.raster._
import org.geotools.factory.CommonFactoryFinder
import java.net.URI

import geotrellis.raster.io.geotiff.GeoTiff
import geotrellis.raster.rasterize.Rasterizer
import geotrellis.spark.clip.ClipToGrid
import geotrellis.spark.clip.ClipToGrid.Predicates
import org.apache.spark.{Partitioner, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.geotools.data.FeatureReader
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.feature.FeatureReaderIterator
import org.locationtech.jts.geom.prep.PreparedGeometryFactory
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable

/**
 *
 * @param uri URI of GRUMP shapefile, stored in EPSG:4326
 * @param crs CRS of queries and return geometries
 */
case class Grump(uri: URI, crs: CRS = LatLng) {
  println(s"GRUMP Shapefile: $uri")

  @transient private lazy val ds = new ShapefileDataStore(uri.toURL)
  @transient private lazy val source = ds.getFeatureSource()
  @transient private lazy val ff = CommonFactoryFinder.getFilterFactory2
  private val needTransform = crs != LatLng
  private val transform = Transform(LatLng, crs)

  private def featureReaderToIterator(sfr: FeatureReader[SimpleFeatureType, SimpleFeature]): Iterator[MultiPolygon] = {
    import scala.collection.JavaConverters._
    new FeatureReaderIterator[SimpleFeature](sfr).asScala.
      map { f => f.getDefaultGeometry.asInstanceOf[MultiPolygon]}.
      map { mp => if (needTransform) mp.reproject(transform) else mp }
  }

  /** Query GRUMP for geometries in bounding box.
   * Geometries are reprojected to crs specified in the constructor
   * @param extent query bounding box
   */
  def query(extent: Extent): Iterator[MultiPolygon] = {
    val filter = ff.bbox("the_geom", extent.xmin, extent.ymin, extent.xmax, extent.ymax, crs.toString)
    val sfr: FeatureReader[SimpleFeatureType, SimpleFeature] = source.getReader(filter)
    featureReaderToIterator(sfr)
  }

  /** Query GRUMP for geometries in bounding box.
   * Geometries are reprojected to crs specified in the constructor
   * @param geom query geometry to filter on
   */
  def query(geom: org.locationtech.jts.geom.MultiPolygon): Iterator[MultiPolygon] = {
    val filter = ff.intersects(ff.property("the_geom"), ff.literal(geom))
    val sfr: FeatureReader[SimpleFeatureType, SimpleFeature] = source.getReader(filter)
    featureReaderToIterator(sfr)
  }

  /** Query GRUMP as rasterized mask of geometries
   * @param geom query polygon in CRS used in constructor
   * @param layout layout for tiling in CRS used in constructor
   * @param part partitioner to apply to keys
   */
  def queryAsMaskRdd(geom: MultiPolygon, layout: LayoutDefinition, part: Partitioner)(implicit sc: SparkSession): RDD[(SpatialKey, Tile)] = {
    val keys = layout.mapTransform.keysForGeometry(geom).toArray
    val keyRdd = sc.sparkContext.parallelize(keys, 1).map( key => (key, key)).partitionBy(part)

    keyRdd.mapValues { key =>
      println(s"Reading GRUMP: $key")
      val keyExtent = layout.mapTransform.keyToExtent(key)
      // geometries will certainly expand outside of keyExtent, meaning we will query some of them many times
      val geoms = this.query(keyExtent)
      val options = Rasterizer.Options(includePartial =  false, sampleType = PixelIsPoint)
      val mask = ArrayTile.empty(BitCellType, layout.tileCols, layout.tileRows)
      val re = RasterExtent(keyExtent, mask.cols, mask.rows)
      for { geom <- geoms} Rasterizer.foreachCellByGeometry(geom, re, options) { (col, row) => mask.set(col, row, 1)}
      mask
    }
  }

  def readAll(partitionCount: Int)(implicit spark: SparkSession): RDD[Geometry] = {
    val sfr: FeatureReader[SimpleFeatureType, SimpleFeature] = source.getReader()
    val it = featureReaderToIterator(sfr).map(_.buffer(0)) // buffer fixes self-intersecting nodes
    spark.sparkContext.parallelize(it.toSeq, partitionCount)
  }
}

object Grump {

  def masksForBoundary(
    rdd: RDD[Geometry],
    layout: LayoutDefinition,
    boundary: MultiPolygon,
    part: Partitioner
  ): RDD[(SpatialKey, Tile)] = {
    // prepared geometries are not serializable
    @transient lazy val prep = PreparedGeometryFactory.prepare(boundary)

    val clipFeature: (Extent, Feature[Geometry, Unit], Predicates) => Option[Feature[Geometry, Unit]] = {
      (extent, feature, pred) =>
        if (prep.intersects(feature.geom)) ClipToGrid.clipFeatureToExtent(extent, feature, pred)
        else None // if feature does not intersect our boundary, discard it
    }

    val featureRdd = rdd.map( g => Feature(g, ()))

    ClipToGrid(featureRdd, layout,clipFeature)
      .groupByKey(part)
      .mapPartitions({ iter =>
        iter.map { case (key, features) =>
          val options = Rasterizer.Options(includePartial = false, sampleType = PixelIsPoint)
          val mask = ArrayTile.empty(BitCellType, layout.tileCols, layout.tileRows)
          val keyExtent = layout.mapTransform.keyToExtent(key)
          val re = RasterExtent(keyExtent, mask.cols, mask.rows)
          for (f <- features) Rasterizer.foreachCellByGeometry(f.geom, re, options) { (col, row) => mask.set(col, row, 1) }
          (key, mask)
        }
      }, preservesPartitioning = true)
  }
}