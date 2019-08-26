package geotrellis.sdg


object CountryDirectory {
  val countries: Array[(String, String)] =
    Array(
      //("oman", "omn"),
      //("djibouti", "dji")
      //("east-timor", "tls"),
      //("luxembourg", "lux"),
      ("belize", "blz"),
      ("nicaragua", "nic"),
      ("honduras", "hnd"),
      ("el salvador", "slv"),
      ("guatemala", "gtm"),
      ("costa rica", "cri"),
      ("panama", "pan")
    )

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
    // any "_" or " " in the name with "-"
      name.toLowerCase.replaceAll("(_| )", "-")

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
