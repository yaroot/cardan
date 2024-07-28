import mill._
import mill.scalalib._
import mill.scalalib.scalafmt.ScalafmtModule

val CatsEffectVersion  = "3.5.4"
val CirceVersion       = "0.14.8"
val DoobieVersion      = "1.0.0-RC4"
val FS2Version         = "3.10.2"
val HikariCPVersion    = "5.1.0"
val KafkaVersion       = "3.7.0"
val Log4CatsVersion    = "2.6.0"
val LogbackVersion     = "1.5.6"
val MUnitVersion       = "0.7.29"
val PostgresqlVersion  = "42.7.3"
val WartremoverVersion = "3.1.8"

val ScalaVersion = "2.13.14"

object Shared {
  object Deps {
    val kafkaClient = Seq(
      ivy"org.apache.kafka:kafka-clients:$KafkaVersion"
    )

    val common = Seq(
      ivy"ch.qos.logback:logback-classic:$LogbackVersion"
    )

    val catsEffect = Seq(
      ivy"org.typelevel::cats-effect:$CatsEffectVersion"
    )

    val postgres = Seq(
      ivy"com.zaxxer:HikariCP:$HikariCPVersion",
      ivy"org.postgresql:postgresql:$PostgresqlVersion"
    )

    val circe = Seq(
      ivy"io.circe::circe-generic:$CirceVersion",
      ivy"io.circe::circe-jawn:$CirceVersion"
    )

    val log4cats = Seq(
      ivy"org.typelevel::log4cats-core:$Log4CatsVersion",
      ivy"org.typelevel::log4cats-slf4j:$Log4CatsVersion"
    )

    val fs2 = Seq(
      ivy"co.fs2::fs2-core:$FS2Version",
      ivy"co.fs2::fs2-io:$FS2Version"
    )

    val doobie = Seq(
      ivy"org.tpolecat::doobie-hikari:$DoobieVersion",
      ivy"org.tpolecat::doobie-postgres:$DoobieVersion"
    )

    val doobiePgCirce = Seq(
      ivy"org.tpolecat::doobie-postgres-circe:$DoobieVersion"
    )

    val wartremover = Seq(
      ivy"org.wartremover::wartremover:$WartremoverVersion"
    )
  }

  val scalacOptions = Seq(
    "-encoding",
    "utf8",
    "-feature",
    "-unchecked",
    "-language:existentials",
    "-language:experimental.macros",
    "-language:higherKinds",
    "-language:implicitConversions",
    "-Xlint:adapted-args",
    "-Xlint:constant",
    "-Xlint:delayedinit-select",
    "-Xlint:deprecation",
    "-Xlint:doc-detached",
    "-Xlint:implicit-recursion",
    "-Xlint:implicit-not-found",
    "-Xlint:inaccessible",
    "-Xlint:infer-any",
    "-Xlint:missing-interpolator",
    "-Xlint:nullary-unit",
    "-Xlint:option-implicit",
    "-Xlint:package-object-classes",
    "-Xlint:poly-implicit-overload",
    "-Xlint:private-shadow",
    "-Xlint:stars-align",
    "-Xlint:strict-unsealed-patmat",
    "-Xlint:type-parameter-shadow",
    "-Xlint:-byname-implicit",
    "-Wdead-code",
    "-Wextra-implicit",
    "-Wnumeric-widen",
    "-Wvalue-discard",
    "-Wunused:nowarn",
    "-Wunused:implicits",
    "-Wunused:explicits",
    "-Wunused:imports",
    "-Wunused:locals",
    "-Wunused:params",
    "-Wunused:patvars",
    "-Wunused:privates",
    "-Xfatal-warnings",
    "-Ymacro-annotations",
    "-Xsource:3",
    "-P:wartremover:traverser:org.wartremover.warts.AsInstanceOf",
    "-P:wartremover:traverser:org.wartremover.warts.EitherProjectionPartial",
    "-P:wartremover:traverser:org.wartremover.warts.Null",
    "-P:wartremover:traverser:org.wartremover.warts.OptionPartial",
    "-P:wartremover:traverser:org.wartremover.warts.Product",
    "-P:wartremover:traverser:org.wartremover.warts.Return",
    "-P:wartremover:traverser:org.wartremover.warts.TryPartial",
    "-P:wartremover:traverser:org.wartremover.warts.Var"
  )
}

trait CommonScalaModule extends ScalaModule with ScalafmtModule {
  override def scalaVersion: T[String] = T(ScalaVersion)
}

object cardan extends CommonScalaModule {
  override def scalacOptions: T[Seq[String]] = T(Shared.scalacOptions)
  override def compileIvyDeps                = T(Shared.Deps.wartremover)
  override def scalacPluginIvyDeps           = T(Shared.Deps.wartremover)

  override def mainClass: T[Option[String]] = T(Some("cardan.Main"))

  override def ivyDeps: T[Agg[Dep]] = T(
    Agg.from(
      Shared.Deps.common ++
        Shared.Deps.doobie ++
        Shared.Deps.doobiePgCirce ++
        Shared.Deps.kafkaClient ++
        Shared.Deps.postgres ++
        Shared.Deps.circe ++
        Shared.Deps.catsEffect ++
        Shared.Deps.fs2 ++
        Shared.Deps.log4cats
    )
  )
}
