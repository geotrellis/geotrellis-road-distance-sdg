package geotrellis.qatiles

import geotrellis.vectortile.{VString, Value}

/** Container for relevant OSM tags on road features */
case class RoadTags(highway: Option[String], surface: Option[String]) {
  /** Assume road is motor road if not explicitly tagged as foot path */
  def isStrictlyMotorRoad: Boolean = highway match {
    case Some(v) if RoadTags.highwayRoadValues.contains(v) => true
    case Some(_) => false
    case None => false
  }

  /** Assume road is motor road if not explicitly tagged as foot path */
  def isPossiblyMotorRoad: Boolean = highway match {
    case Some(v) if RoadTags.highwayPathValues.contains(v) => false
    case Some(_) => true
    case None => false
  }

  /** Surface explicitly support all weather */
  def isStrictlyAllWeather: Boolean = surface.exists(v => RoadTags.surfaceAllWeatherValues.contains(v))

  /** Surface is not explicitly restricted or undefined */
  def isPossiblyAllWeather: Boolean = surface.isEmpty || surface.exists( v => ! RoadTags.surfaceRestricted.contains(v))
}

object RoadTags {
  def apply(tags: Map[String, Value]): RoadTags = {
    new RoadTags(
      tags.get("highway").collect({ case VString(s) => s }),
      tags.get("surface").collect({ case VString(s) => s })
    )
  }

  /** Highway values that are motor roads */
  final val highwayRoadValues = Array(
    "motorway", "trunk", "primary", "secondary", "tertiary", "unclassified",
    "motorway_link", "trunk_link", "primary_link", "secondary_link", "tertiary_link", "living_street", "service"
  )

  /** Highway values that presents pedestrian road */
  final val highwayPathValues = Array(
    "path", "steps", "bridleway", "footway", "track")

  /** Surface values we consider all weather */
  final val surfaceAllWeatherValues = Array(
    "paved",  "asphalt",  "concrete", "concrete:lanes", "concrete:plates", "paving_stones",
    "sett", "unhewn_cobblestone", "cobblestone", "metal", "wood")

  /** Surface values that are unusable during some parts of the year */
  final val surfaceRestricted = Array(
    "compacted",  "woodchips", "grass_paver", "grass", "dirt", "earth", "mud", "ground",
    "fine_gravel", "gravel", "gravel_turf", "pebblestone", "salt", "sand", "snow", "unpaved")
}

