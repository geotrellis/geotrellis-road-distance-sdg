package geotrellis.sdg

case class PopulationSummary(
  country: Country,
  population: Double,
  populationNearRoads: Double,
  regionsRead: Long
) {
  def combine(other: PopulationSummary): PopulationSummary = {
    require(this.country == other.country, s"Can't combine across ${this.country} and ${other.country}")
    PopulationSummary(this.country,
      this.population + other.population,
      this.populationNearRoads + other.populationNearRoads,
      this.regionsRead + other.regionsRead)
  }
}