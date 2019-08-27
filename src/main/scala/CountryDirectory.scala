package geotrellis.sdg

import scalaj.http.Http


object CountryDirectory {
  val countries: Array[(String, String)] = {
    val request = Http("https://un-sdg.s3.amazonaws.com/countries.csv")
    val lines = request.asString.body

    // TODO: Come up with a better way of creating the array
    lines
      .split("\n")
      .map { _.split(",").map { _.trim } }
      .map { arr =>
        val lowerCaseName = arr(0).toLowerCase.replaceAll(" ", "_")
        val lowerCaseCode = arr(1).toLowerCase

        (lowerCaseName, lowerCaseCode)
      }
  }

  def codeToName(code: String): String = {
    val lowerCaseCode: String = code.toLowerCase

    val filteredCountries: Array[(String, String)] =
      countries.filter { case (_, code) =>
        code == lowerCaseCode
      }

    if (filteredCountries.isEmpty)
      throw new Error(s"Could not find name for country code: $lowerCaseCode")
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
}
