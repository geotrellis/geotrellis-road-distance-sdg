package geotrellis.sdg

import io.circe._, io.circe.generic.semiauto._

case class OutputProperties(
  code: String,
  name: String,
  pop: Double,
  pop_served: Double,
  pct_served: Double
)

object OutputProperties {
  implicit val fooDecoder: Decoder[OutputProperties] = deriveDecoder[OutputProperties]
  implicit val fooEncoder: Encoder[OutputProperties] = deriveEncoder[OutputProperties]
}