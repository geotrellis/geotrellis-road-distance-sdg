package geotrellis.sdg

import geotrellis.qatiles.RoadTags
import org.apache.spark.util.AccumulatorV2
import geotrellis.vector._
import cats.implicits._


class RoadHistogram(var map: Map[RoadHistogram.Key, Double])
  extends AccumulatorV2[RoadHistogram.Row, RoadHistogram.Result]
{
  def isZero: Boolean = map.isEmpty
  def value: RoadHistogram.Result = map
  def reset(): Unit = { map = Map.empty}
  def copy() = new RoadHistogram(map)
  def merge(other: AccumulatorV2[(RoadHistogram.Row), RoadHistogram.Result]): Unit =
    this.map = this.map |+| other.value
  def add(v: RoadHistogram.Row): Unit = {
    val (length, included, tags) = v
    val key = RoadHistogram.Key(tags.highway.getOrElse("<empty>"), tags.surface.getOrElse("<empty>"), included)
    val km: Double = map.getOrElse(key, 0.0) + length
    map = map.updated(key, km)
  }
}

object RoadHistogram {
  type Row = (Double, Boolean, RoadTags)
  type Result = Map[Key, Double]

  case class Key(highway: String, surface: String, included: Boolean)
  object Key {
    import _root_.io.circe._, _root_.io.circe.generic.semiauto._
    implicit val fooEncoder: Encoder[Key] = deriveEncoder[Key]
    implicit val fooDecoder: Decoder[Key] = deriveDecoder[Key]
  }

  def empty = new RoadHistogram(Map.empty)
}
