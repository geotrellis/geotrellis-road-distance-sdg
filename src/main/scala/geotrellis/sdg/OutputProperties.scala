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
  breaks: Array[Double]
)

object OutputProperties {
  implicit val fooDecoder: Decoder[OutputProperties] = deriveDecoder[OutputProperties]
  implicit val fooEncoder: Encoder[OutputProperties] = deriveEncoder[OutputProperties]

  def apply(country: Country, summary: PopulationSummary, breaks: Array[Double]): OutputProperties =
    new OutputProperties(
      code = country.code,
      name = country.name,
      pop = summary.total,
      pop_urban = summary.urban,
      pop_rural = summary.rural,
      pop_served = summary.ruralServed,
      pct_served = (summary.ruralServed / summary.rural),
      breaks = breaks
    )
}