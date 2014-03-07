organization := "me.lessis"

name := "bunnyhop"

version := "0.1.0-SNAPSHOT"

description := "A rabbitmq interface that speaks scala"

libraryDependencies ++= Seq(
  "com.rabbitmq" % "amqp-client" % "3.2.3",
  "org.scalatest" %% "scalatest" % "1.9.1" % "test")

crossScalaVersions := Seq("2.9.3", "2.10.3")

scalaVersion := crossScalaVersions.value.head

scalacOptions += Opts.compile.deprecation
