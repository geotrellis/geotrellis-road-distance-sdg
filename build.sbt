name := "geotrellis-road-distance-sdg"
version := "0.1"
scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.azavea" %% "osmesa" % "0.1.0",
  "com.azavea" %% "osmesa-common" % "0.1.0",
  "org.apache.spark" %% "spark-core" % "2.3.0" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.3.2" % "provided",
  "com.monovore" %% "decline" % "0.5.0"
)

externalResolvers := Seq(
  DefaultMavenRepository,
  Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns)
)
