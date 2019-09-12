package geotrellis.sdg


case class PopulationSummary(
  population: Double,
  populationNearRoads: Double,
  regionsRead: Long
) {
  def combine(other: PopulationSummary): PopulationSummary = {
    // Polygonal summary can produce NaN for regions of NoData
    def nanToZero(d: Double): Double = if (d.isNaN) 0 else d
    PopulationSummary(
      nanToZero(this.population) + nanToZero(other.population),
      nanToZero(this.populationNearRoads) + nanToZero(other.populationNearRoads),
      this.regionsRead + other.regionsRead)
  }

  def toOutput(country: Country): OutputProperties = {
    OutputProperties(country.code, country.name, population, populationNearRoads, populationNearRoads/population)
  }
}

