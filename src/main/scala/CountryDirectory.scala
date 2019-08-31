package geotrellis.sdg

import java.net.URI

import geotrellis.qatiles.OsmQaTiles.BASE_URL
import scalaj.http.Http


object CountryDirectory {
  val countries: Array[(String, String)] = {
    val lines = Resource.lines("countries.csv")

    lines
      .map { _.split(",").map { _.trim } }
      .map { arr =>
        val name = arr(0).toLowerCase.replaceAll(" ", "_")
        val code = arr(1).toUpperCase()

        (name, code)
      }.toArray
  }

  def codeToName(code: String): String = {
    val filteredCountries: Array[(String, String)] =
      countries.filter { case (_, c) =>  c == code }

    if (filteredCountries.isEmpty)
      throw new Error(s"Could not find name for country code: $code")
    else
      filteredCountries.head._1
  }

  def nameToCode(name: String): String = {
    val lowerCaseName: String =
      // convert the name to lowercase, and replace
      // any "-" or " " in the name with "_"
      name.toLowerCase.replaceAll("(-| )", "_")

    val filteredCountries: Array[(String, String)] =
      countries.filter { case (name, _) =>
        name == lowerCaseName
      }

    if (filteredCountries.isEmpty)
      throw new Error(s"Could not find country code for name: $lowerCaseName")
    else
      filteredCountries.head._2
  }

  def makeHdfsCpCommands: Seq[String] = {
    //Some scripting to copy data in place
    val BASE_URL: String = "s3://mapbox/osm-qa-tiles-production/latest.country/"

    CountryDirectory.countries.map{ case (name, code) =>
      val name = CountryDirectory.codeToName(code)
      val source = s"${BASE_URL}${name.toLowerCase.replace(" ", "_")}.mbtiles.gz"
      val target = s"/$code.mbtiles.gz"
      s"hdfs dfs -cp $source $target"
    }
  }
}
