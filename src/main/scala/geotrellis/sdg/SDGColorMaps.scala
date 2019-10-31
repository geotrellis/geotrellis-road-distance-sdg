package geotrellis.sdg

import geotrellis.raster._
import geotrellis.raster.render.{ClassBoundaryType, GreaterThan, LessThanOrEqualTo}

object SDGColorMaps {

  val MAX_WORLD_POP_PIXEL_VALUE = 27803
  // Ramp provided by Jeff to match current vector tile ramp in use
  val orange5 = ColorRamp(
    0xffffd4ff,
    0xfed98eff,
    0xfe9929ff,
    0xd95f0eff,
    0x993404ff
  )

  val orange9 = ColorRamp(
    0xfff5ebff,
    0xfee6ceff,
    0xfdd0a2ff,
    0xfdae6bff,
    0xfd8d3cff,
    0xf16913ff,
    0xd94801ff,
    0xa63603ff,
    0x7f2704ff
  )

  val globalBreaks = Array(
    0.5,
    1,
    2,
    5,
    10,
    50,
    100,
    1000,
    33000
  )
  val global = orange9.toColorMap(globalBreaks)

  def forgottenPop(histogram: Histogram[Double]): (ColorMap, Array[Double]) = {
    val ramp = orange9
    // TODO: Better handle this if we ever use this method again.
    //       Apparently at least one country has a None value for one of these.
    val Some((min, max)) = histogram.minMaxValues
    val width = (max - min) / ramp.numStops
    val linearBreaks = (1 to ramp.numStops - 1).map { v => v * width }.toArray

    // Log scale - conversion from:
    // https://stackoverflow.com/questions/19472747/convert-linear-scale-to-logarithmic
    // scale start value based on how many powers of ten the max has to give a consistent
    // log scale for a zero bounded ramp
    // The "minus 1" is an offset to attempt to keep the values in the higher breaks to a minimum,
    // it effectively shifts the scale right to slightly larger low end breaks
    val logMin = if (min == 0.0) width / math.pow(10, math.floor(math.log10(max)) - 1) else min
    val x1 = max
    val x2 = logMin
    val y1 = max
    val y2 = logMin
    val b = math.log(y1/y2) / (x1-x2)
    val a = y1 / math.exp(b * x1)
    val breaks = linearBreaks.map { x =>
      a * math.exp(b * x)
    } :+ max

    // Include min to fully describe range of values (e.g. for a viz legend)
    (ramp.toColorMap(breaks), min +: breaks)
  }
}
