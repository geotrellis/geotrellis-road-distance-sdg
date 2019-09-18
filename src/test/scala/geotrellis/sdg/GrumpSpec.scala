package geotrellis.sdg

import java.net.URI

import geotrellis.layer.ZoomedLayoutScheme
import geotrellis.proj4.LatLng
import geotrellis.vector._
import org.scalatest._
import geotrellis.spark.testkit.TestEnvironment
import org.apache.spark.HashPartitioner

class GrumpSpec extends WordSpec with Matchers with TestEnvironment {
  val uri = new URI("file:/Users/eugene/Downloads/grump-v1-urban-ext-polygons-rev01-shp/global_urban_extent_polygons_v1.01.shp")
  // val uri = new URI("https://un-sdg.s3.amazonaws.com/data/grump-v1.01/global_urban_extent_polygons_v1.01.shp")

  "A Grump" when {
    "constructed from URI" should {
      val grump = new Grump(uri)

      "read shapefile" in {
        val extent = Extent(-81.12605576, 35.31848703, -79.00058446, 37.65116824)
        grump.query(extent).toList should not be empty
      }

      "read masks as RDD[(SpatialKey, Tile]) for country" in {
        val mwiBorder = Country.allCountries("MWI").geom
        val scheme = ZoomedLayoutScheme(LatLng, 1024)
        val layout = scheme.levelForZoom(6).layout
        val rdd = grump.queryAsMaskRdd(mwiBorder, layout, new HashPartitioner(16))
        rdd.count should not be 0
      }
    }
  }



}