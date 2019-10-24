package geotrellis.sdg

import geotrellis.raster.{ColorMap, ColorRamp, Histogram}

object SDGColorMaps {
  // Ramp provided by Jeff to match current vector tile ramp in use
  val orange = ColorRamp(
    0xffffd4ff,
    0xfed98eff,
    0xfe9929ff,
    0xd95f0eff,
    0x993404ff
  )

  def forgottenPop(histogram: Histogram[Double]): (ColorMap, Array[Double]) = {
    val Some((min, max)) = histogram.minMaxValues()
    val width = (max - min) / orange.numStops
    val middleBreaks = (1 to orange.numStops).map { v => v * width }
    val breaks = middleBreaks :+ max
    (orange.toColorMap(breaks.toArray), (min +: breaks).toArray)
  }
}
