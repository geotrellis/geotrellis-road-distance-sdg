package geotrellis.sdg


case class PopulationSummary(
  total: Double,
  urban: Double,
  rural: Double,
  ruralServed: Double,
  ruralLost: Double
) {
  require(rural.isNaN || rural >= 0)

  def combine(other: PopulationSummary): PopulationSummary = {
    // Polygonal summary can produce NaN for regions of NoData
    def nanToZero(d: Double): Double = if (d.isNaN) 0 else d
    PopulationSummary(
      nanToZero(this.total) + nanToZero(other.total),
      nanToZero(this.urban) + nanToZero(other.urban),
      nanToZero(this.rural) + nanToZero(other.rural),
      nanToZero(this.ruralServed) + nanToZero(other.ruralServed),
      nanToZero(this.ruralLost) + nanToZero(other.ruralLost)
    )
  }

  def report: String =
    s"""PopulationSummary:
       | total = $total
       | urban = $urban
       | rural = $rural
       | served = ${ruralServed / rural}
       | lost = ${ruralLost / rural}
       |""".stripMargin
}

object PopulationSummary {
  final val empty = PopulationSummary(Double.NaN, Double.NaN, Double.NaN, Double.NaN, Double.NaN)
}
