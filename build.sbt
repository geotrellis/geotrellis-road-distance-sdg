name := "geotrellis-road-distance-sdg"
version := "2.1"
scalaVersion := "2.11.12"
organization := "geotrellis"

libraryDependencies ++= Seq(
  "com.uber" % "h3" % "3.6.0",
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "3.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-layer" % "3.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-s3" % "3.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-shapefile" % "3.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "3.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-s3-spark" % "3.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark-testkit" % "3.1.0",
  "com.azavea" %% "vectorpipe" % "2.0.0-M1",
  "org.apache.spark" %% "spark-core" % "2.4.1" % Provided,
  "org.apache.spark" %% "spark-hive" % "2.4.1" % Provided,
  "org.locationtech.geomesa" %% "geomesa-spark-jts" % "2.3.0",
  "org.log4s" %% "log4s" % "1.8.2",
  "org.tpolecat" %% "doobie-core" % "0.5.2",
  "org.xerial" % "sqlite-jdbc" % "3.28.0",
  // "org.geotools" % "gt-geopkg" % "21.2", // GeoTools version currently used by GT 3.1.0
  "org.geotools" % "gt-epsg-hsql" % "21.2",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9",
  "com.monovore" %% "decline" % "0.5.0",
  "org.scalatest" %% "scalatest" % "3.0.8" % Test
)

externalResolvers := Seq(
  DefaultMavenRepository,
  "OSGeo" at "http://download.osgeo.org/webdav/geotools/",
  "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/",
  "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/",
  Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns),
  Resolver.bintrayRepo("azavea", "geotrellis"),
  Resolver.bintrayRepo("azavea", "maven")
)

test in assembly := {}

assemblyShadeRules in assembly := {
  val shadePackage = "geotrellis.sdg.shaded"
  val gtVersion = "3.1.0"
  Seq(
    ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
      .inLibrary("com.networknt" % "json-schema-validator" % "0.1.7").inAll,
    ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-spark" % gtVersion).inAll
  )
}

assemblyMergeStrategy in assembly := {
  case "reference.conf" => MergeStrategy.concat
  case "application.conf" => MergeStrategy.concat
  case PathList("META-INF", "services", "org.opengis.referencing.crs.CRSAuthorityFactory") => MergeStrategy.concat
  case PathList("META-INF", xs@_*) =>
    xs match {
      case ("MANIFEST.MF" :: Nil) => MergeStrategy.discard
      // Concatenate everything in the services directory to keep GeoTools happy.
      case ("services" :: _ :: Nil) =>
        MergeStrategy.concat
      // Concatenate these to keep JAI happy.
      case ("javax.media.jai.registryFile.jai" :: Nil) | ("registryFile.jai" :: Nil) | ("registryFile.jaiext" :: Nil) =>
        MergeStrategy.concat
      case (name :: Nil) => {
        // Must exclude META-INF/*.([RD]SA|SF) to avoid "Invalid signature file digest for Manifest main attributes" exception.
        if (name.endsWith(".RSA") || name.endsWith(".DSA") || name.endsWith(".SF"))
          MergeStrategy.discard
        else
          MergeStrategy.first
      }
      case _ => MergeStrategy.first
    }
  case _ => MergeStrategy.first
}

sparkInstanceCount          := 64
sparkMasterType             := "m4.4xlarge"
sparkCoreType               := "m4.4xlarge"
sparkMasterPrice            := Some(1.00)
sparkCorePrice              := Some(1.00)
sparkEmrRelease             := "emr-5.26.0"
sparkAwsRegion              := "us-east-1"
sparkSubnetId               := Some("subnet-4f553375")
sparkS3JarFolder            := s"s3://un-sdg/jars/${Environment.user}"
sparkS3LogUri               := Some(s"s3://un-sdg/logs/${Environment.user}/")
sparkClusterName            := s"geotrellis-road-sdg-${Environment.user}"
sparkEmrServiceRole         := "EMR_DefaultRole"
sparkInstanceRole           := "EMR_EC2_DefaultRole"
sparkEmrApplications        := Seq("Spark", "Zeppelin", "Ganglia")
sparkMasterEbsSize          := Some(64)
sparkCoreEbsSize            := Some(64)
sparkJobFlowInstancesConfig := sparkJobFlowInstancesConfig.value.withEc2KeyName("geotrellis-emr")

import com.amazonaws.services.elasticmapreduce.model.Application
sparkRunJobFlowRequest      := sparkRunJobFlowRequest.value.withApplications(
  Seq("Spark", "Ganglia").map(a => new Application().withName(a)):_*
)

import sbtlighter.EmrConfig
sparkEmrConfigs := Seq(
  EmrConfig("spark").withProperties(
    "maximizeResourceAllocation" -> "true"),
  EmrConfig("spark-defaults").withProperties(
  "spark.driver.maxResultSize" -> "2G",
  "spark.executor.maxResultSize" -> "2G",
  "spark.dynamicAllocation.enabled" -> "true",
  "spark.shuffle.service.enabled" -> "true",
  "spark.shuffle.compress" -> "true",
  "spark.shuffle.spill.compress" -> "true",
  "spark.rdd.compress" -> "true",
  "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC"),
  EmrConfig("yarn-site").withProperties(
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)

initialCommands in console :=
"""
import geotrellis.proj4._
import geotrellis.vector._
import geotrellis.raster._
import geotrellis.layer._
import geotrellis.sdg._
"""