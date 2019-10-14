package geotrellis.sdg

import geotrellis.raster._


case class SummaryRegion(
  pop: RasterRegion,
  roadMask: Option[Tile],
  grumpMask: Option[Tile]
) {
  roadMask.foreach { tile =>
    require(pop.dimensions == tile.dimensions,
      s"pop: ${pop.dimensions} road mask: ${tile.dimensions}")
  }

  grumpMask.foreach { tile =>
    require(pop.dimensions == tile.dimensions,
      s"pop: ${pop.dimensions} grump mask: ${tile.dimensions}")
  }

  /** Rural population more than 2km from the road */
  def forgottenPopTile: Option[Tile] = {
    pop.raster.map { popRaster =>
      val pop = popRaster.tile.band(0)
      val roads = roadMask.getOrElse(BitArrayTile.empty(pop.cols, pop.rows))
      val grump = grumpMask.getOrElse(BitArrayTile.empty(pop.cols, pop.rows))

      pop.localMask(grump.localOr(roads), readMask = 1, writeMask = NODATA)
    }
  }

  /** Urban or w/in 2km of allweather road */
  def servedPopTile: Option[Tile] = {
    pop.raster.map { popRaster =>
      val pop = popRaster.tile.band(0)
      val roads = roadMask.getOrElse(BitArrayTile.empty(pop.cols, pop.rows))
      val grump = grumpMask.getOrElse(BitArrayTile.empty(pop.cols, pop.rows))

      pop.localMask(grump.localNot.localAnd(roads), readMask = 0, writeMask = NODATA)
    }
  }


  def summary: Option[PopulationSummary] = {
    def cellSum(tile: Tile): Double = {
      var ret: Double = Double.NaN
      tile.foreachDouble({ x =>
        require(isNoData(x) || x >= 0, s"pop = $x")
        if (isData(ret) && isData(x)) ret += x else if (isNoData(ret)) ret = x
      })
      ret
    }

    pop.raster.map { popRaster =>
      val pop = popRaster.tile.band(0)
      val roads = roadMask.getOrElse(BitArrayTile.empty(pop.cols, pop.rows))
      val grump = grumpMask.getOrElse(BitArrayTile.empty(pop.cols, pop.rows))
      // how do I get the mask for rural areas far away from roads: GRUMP == 0 && ROADs == 0
      PopulationSummary(
        total = cellSum(pop),
        urban = cellSum(pop.localMask(grump, readMask = 0, writeMask = NODATA)),
        rural = cellSum(pop.localMask(grump, readMask = 1, writeMask = NODATA)),
        ruralServed = cellSum(pop.localMask(grump.localNot.localAnd(roads), readMask = 0, writeMask = NODATA)),
        ruralLost = cellSum(pop.localMask(grump.localOr(roads), readMask = 1, writeMask = NODATA)))
    }
  }
}
