package geotrellis.sdg

import geotrellis.layer._
import geotrellis.vector._
import geotrellis.vectortile.VectorTile
import geotrellis.spark.store.s3._

import org.apache.spark.rdd.RDD

import software.amazon.awssdk.services.s3.model.ObjectCannedACL

import scala.util.Try

import java.io.ByteArrayOutputStream
import java.util.zip.GZIPOutputStream

object VTileUtil {

  def keyToLayout[G <: Geometry, D](features: RDD[Feature[Geometry, D]], layout: LayoutDefinition): RDD[(SpatialKey, (SpatialKey, Feature[Geometry, D]))] = {
    features.flatMap{ feat =>
      val g = feat.geom
      val keys = layout.mapTransform.keysForGeometry(g)
      keys.flatMap{ k =>
        val SpatialKey(x, y) = k
        if (x < 0 || x >= layout.layoutCols || y < 0 || y >= layout.layoutRows) {
          println(s"Geometry $g exceeds layout bounds in $k (${Try(layout.mapTransform(k))})")
          None
        } else {
          Some(k -> (k, feat))
        }
      }
    }
  }

  def save(vectorTiles: RDD[(SpatialKey, VectorTile)], zoom: Int, bucket: String, prefix: String) = {
    vectorTiles
      .mapValues { tile =>
        val byteStream = new ByteArrayOutputStream()

        try {
          val gzipStream = new GZIPOutputStream(byteStream)
          try {
            gzipStream.write(tile.toBytes)
          } finally {
            gzipStream.close()
          }
        } finally {
          byteStream.close()
        }

        byteStream.toByteArray
      }
      .saveToS3(
        { sk: SpatialKey => s"s3://${bucket}/${prefix}/${zoom}/${sk.col}/${sk.row}.mvt" },
        putObjectModifier = { request =>
          request
            .toBuilder()
            .contentEncoding("gzip")
            .acl(ObjectCannedACL.PUBLIC_READ)
            .build()
        }
      )
    }

}