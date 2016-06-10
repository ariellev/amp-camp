

name := "wiki"
version := "1.0"
//scalaHome := Some(file("/usr/local/Cellar/scala/2.11.7/libexec"))
scalaVersion := "2.11.7"
libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-streaming-twitter_2.11" % "1.6.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "1.6.0"
libraryDependencies += "org.tachyonproject" % "tachyon-client" % "0.8.2"

mainClass in (Compile, run) := Some("org.sandbox.wiki.Wiki")
mainClass in (Compile, packageBin) := Some("org.sandbox.wiki.Wiki")