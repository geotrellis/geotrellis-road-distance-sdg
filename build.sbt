name := "geotrellis-road-distance-sdg"
version := "0.1"
scalaVersion := "2.11.11"
organization := "geotrellis"

libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-vectortile" % "3.0.0-SNAPSHOT",
  "org.locationtech.geotrellis" %% "geotrellis-layer" % "3.0.0-SNAPSHOT",
  "org.xerial" % "sqlite-jdbc" % "3.28.0",
  "com.azavea.geotrellis" % "geotrellis-contrib-vlm_2.11" % "3.17.1",
  //"com.azavea" %% "osmesa" % "0.3.0",
  //"com.azavea" %% "osmesa-common" % "0.3.0",
  "org.apache.spark" %% "spark-core" % "2.3.2",// % "provided",
  "org.apache.spark" %% "spark-hive" % "2.3.2",// % "provided",
  "com.monovore" %% "decline" % "0.5.0",
  "org.locationtech.geomesa" %% "geomesa-spark-jts" % "2.3.0",
  "org.locationtech.jts" % "jts-core" % "1.16.1",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.9"
)

externalResolvers := Seq(
  DefaultMavenRepository,
  "locationtech-releases" at "https://repo.locationtech.org/content/repositories/releases/",
  "locationtech-snapshots" at "https://repo.locationtech.org/content/repositories/snapshots/",
  Resolver.file("local", file(Path.userHome.absolutePath + "/.ivy2/local"))(Resolver.ivyStylePatterns),
  Resolver.bintrayRepo("azavea", "geotrellis")
)

test in assembly := {}

assemblyShadeRules in assembly := {
  val shadePackage = "geotrellis.sdg.shaded"
  val gtVersion = "2.0.0"
  Seq(
    ShadeRule.rename("com.fasterxml.jackson.**" -> s"$shadePackage.com.fasterxml.jackson.@1")
      .inLibrary("com.networknt" % "json-schema-validator" % "0.1.7").inAll,
    ShadeRule.rename("org.apache.avro.**" -> s"$shadePackage.org.apache.avro.@1")
      .inLibrary("com.azavea.geotrellis" %% "geotrellis-spark" % gtVersion).inAll
  )
}

assemblyMergeStrategy in assembly := {
  case s if s.startsWith("META-INF/services") => MergeStrategy.concat
  case "reference.conf" | "application.conf"  => MergeStrategy.concat
  case "META-INF/MANIFEST.MF" | "META-INF\\MANIFEST.MF" => MergeStrategy.discard
  case "META-INF/ECLIPSE.RSA" | "META-INF/ECLIPSE.SF" => MergeStrategy.discard
  case "META-INF/ECLIPSE_.RSA" | "META-INF/ECLIPSE_.SF" => MergeStrategy.discard
  case _ => MergeStrategy.first
}

sparkInstanceCount          := 5
sparkMasterType             := "m4.2xlarge"
sparkCoreType               := "m4.2xlarge"
sparkMasterPrice            := Some(0.12)
sparkCorePrice              := Some(0.12)
sparkEmrRelease             := "emr-5.19.0"
sparkAwsRegion              := "us-east-1"
sparkSubnetId               := Some("subnet-4f553375")
sparkS3JarFolder            := s"s3://un-sdg/jars/${Environment.user}"
sparkClusterName            := s"geotrellis-road-sdg-${Environment.user}"
sparkEmrServiceRole         := "EMR_DefaultRole"
sparkInstanceRole           := "EMR_EC2_DefaultRole"
sparkEmrApplications        := Seq("Spark", "Zeppelin")
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
  "spark.driver.memory" -> "4G",
  "spark.executor.memory" -> "4G",
  "spark.driver.maxResultSize" -> "8G",
  "spark.executor.maxResultSize" -> "5G",
  "spark.dynamicAllocation.enabled" -> "true",
  "spark.shuffle.service.enabled" -> "true",
  "spark.shuffle.compress" -> "true",
  "spark.shuffle.spill.compress" -> "true",
  "spark.rdd.compress" -> "true",
  "spark.default.parallelism" -> "15000",
  //"spark.yarn.am.memory" -> "4g",
  //"spark.yarn.am.memoryOverhead" -> "4g",
  "spark.executor.extraJavaOptions" -> "-XX:+UseParallelGC -Dgeotrellis.s3.threads.rdd.write=64"),
  EmrConfig("yarn-site").withProperties(
    "yarn.resourcemanager.am.max-attempts" -> "1",
    "yarn.nodemanager.vmem-check-enabled" -> "false",
    "yarn.nodemanager.pmem-check-enabled" -> "false"
  )
)
