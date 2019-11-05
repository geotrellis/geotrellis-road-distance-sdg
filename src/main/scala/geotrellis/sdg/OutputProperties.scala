package geotrellis.sdg

import io.circe._, io.circe.generic.semiauto._

case class OutputProperties(
  code: String,
  name: String,
  pop: Double,
  pop_urban: Double,
  pop_rural: Double,
  pop_served: Double,
  pct_served: Double,
  roads_total_km: Double,
  roads_included_km: Double,
  breaks: Array[Double]
)

object OutputProperties {
  implicit val fooDecoder: Decoder[OutputProperties] = deriveDecoder[OutputProperties]
  implicit val fooEncoder: Encoder[OutputProperties] = deriveEncoder[OutputProperties]

  def apply(country: Country, summary: PopulationSummary, breaks: Array[Double], roadHistogram: RoadHistogram): OutputProperties = {
    val included = roadHistogram.value.filter(_._1.included == true).map(_._2).sum
    val total = roadHistogram.value.map(_._2).sum

    new OutputProperties(
      code = country.code,
      name = country.name,
      pop = summary.total,
      pop_urban = summary.urban,
      pop_rural = summary.rural,
      pop_served = summary.ruralServed,
      pct_served = (summary.ruralServed / summary.rural),
      roads_total_km = total,
      roads_included_km = included,
      breaks = breaks
    )
  }
}