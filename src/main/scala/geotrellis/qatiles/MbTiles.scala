
/*
 * Copyright 2018 Azavea
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package geotrellis.qatiles

import java.io.ByteArrayInputStream
import java.util.zip.GZIPInputStream

import cats.effect.IO
import doobie._
import doobie.implicits._
import geotrellis.layer.{SpatialKey, TileBounds, ZoomedLayoutScheme}
import geotrellis.vectortile.VectorTile
import java.io.File


class MbTiles(dbPath: File, scheme: ZoomedLayoutScheme) {
  val xa = Transactor.fromDriverManager[IO](
    "org.sqlite.JDBC",
    s"jdbc:sqlite:${dbPath.toString}",
    "", ""
  )

  // https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#content-1
  private def flipRow(row: Int, zoom: Int): Int = (1<<zoom) - 1 - row

  def fetch(zoom: Int, col: Int, row: Int): Option[MbTiles.Row] = {
    fetchQuery(zoom, col, flipRow(row, zoom)).transact(xa).unsafeRunSync.map(_.decode(scheme))
  }

  private def fetchQuery(zoom: Int, col: Int, row: Int): ConnectionIO[Option[MbTiles.InternalRow]] =
    sql"""
    select zoom_level, tile_column, tile_row, tile_data
    from tiles
    where zoom_level=$zoom and tile_column=$col and tile_row=$row
    """.query[MbTiles.InternalRow].option

  def fetchBounds(zoom: Int, bounds: TileBounds): Seq[MbTiles.Row] =
    fetchBoundsQuery(zoom, bounds).transact(xa).unsafeRunSync.map(_.decode(scheme))

  private def fetchBoundsQuery(zoom: Int, bounds: TileBounds): ConnectionIO[List[MbTiles.InternalRow]] =
    sql"""
    select zoom_level, tile_column, tile_row, tile_data
    from tiles
    where zoom_level=$zoom
    and tile_column >= ${bounds.colMin} and tile_column <= ${bounds.colMax}
    and tile_row >= ${flipRow(bounds.rowMin, zoom)} and tile_row <= ${flipRow(bounds.rowMax, zoom)}
    """.query[MbTiles.InternalRow].to[List]


  def allTiles(zoom: Int): Iterator[MbTiles.Row] =
    allTilesQuery.transact(xa).unsafeRunSync.map(_.decode(scheme))

  private def allTilesQuery: ConnectionIO[Iterator[MbTiles.InternalRow]] =
    sql"""
    select zoom_level, tile_column, tile_row, tile_data
    from tiles
    """.query[MbTiles.InternalRow].to[Iterator]


  def allKeys(zoom: Int): Iterator[SpatialKey] = {
    allKeysQuery(zoom).transact(xa).unsafeRunSync().
      map({ case SpatialKey(col, row) => SpatialKey(col, flipRow(row, zoom))})
  }

  private def allKeysQuery(zoom: Int): ConnectionIO[Iterator[SpatialKey]] = {
    sql"""
    select tile_column, tile_row from tiles
    where zoom_level=$zoom
    """.query[SpatialKey].to[Iterator]
  }

  private def findForBounds(zoom: Int, bounds: TileBounds): ConnectionIO[List[MbTiles.InternalRow]] =
    sql"""
    select zoom_level, tile_column, tile_row, tile_data
    from tiles
    where zoom_level=$zoom
    and tile_column >= ${bounds.colMin} and tile_column <= ${bounds.colMax}
    and tile_row >= ${bounds.rowMin} and tile_row <= ${bounds.rowMax}
    """.query[MbTiles.InternalRow].to[List]

}

object MbTiles {
  case class Row(zoom: Int, key: SpatialKey, tile: VectorTile)

  private case class InternalRow(zoom: Int, col: Int, row: Int, pbf: Array[Byte]){
    def decode(scheme: ZoomedLayoutScheme): Row = {
      val flipRow = (1<<zoom) - 1 - row
      val extent = scheme.levelForZoom(zoom).layout.mapTransform.keyToExtent(col, flipRow)
      val is = new ByteArrayInputStream(pbf)
      val gzip = new GZIPInputStream(is)
      val bytes = sun.misc.IOUtils.readFully(gzip, -1, true)
      val tile = VectorTile.fromBytes(bytes, extent)
      is.close()
      gzip.close()

      Row(zoom, SpatialKey(col, flipRow), tile)
    }
  }
}