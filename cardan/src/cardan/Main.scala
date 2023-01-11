package cardan

import cats.implicits.*
import cats.effect.*
import io.circe.generic.JsonCodec
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.{Stream, Pipe}
import io.circe.Json
import org.apache.kafka.clients.producer.KafkaProducer

import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.*

object Main extends IOApp.Simple {
  implicit val loggerFactory: LoggerFactory[IO] = LoggerFactory.default[IO]
  val logger0                                   = org.slf4j.LoggerFactory.getLogger("main")

  override def run: IO[Unit] = {
    val fx = for {
      config       <- Stream.eval(readConfig)
      javaProducer <- Stream.resource(newKafkaProducer(config.kafka))
      xa           <- Stream.resource(newPgPool(config.postgres))
      pgReader      = PgReader[IO](config.postgres, xa)
      changeFilter  = ChangeFilter(config.topic)
      _            <- Stream.eval(pgReader.resetDataSlot)
      sink          = KafkaSink[IO, Json, CapturedValue, String](javaProducer)
      _            <- flow(pgReader, sink, changeFilter, config)
    } yield ()

    fx.compile.drain
  }

  def configFile: IO[String] = IO.envForIO.get("CONFIG_FILE").map(_.getOrElse("./app.json"))

  def flow(
    pgReader: PgReader[IO],
    producer: KafkaSink[IO, Json, CapturedValue, String],
    changeFilter: ChangeFilter,
    config: Configuration
  ): Stream[IO, Unit] = {
    val kBatchSize  = config.kafka.batch_size
    val pgBatchSize = config.postgres.batch_buffer * config.postgres.batch_size

    val source: Stream[IO, Record] = Stream
      .eval(pgReader.readBatch)
      .repeat
      .flatMap(Stream.emits)
      .groupWithin(pgBatchSize, 10.millis)
      .unchunks

    val sink: Pipe[IO, Record, Unit] =
      _.map(changeFilter.pass).unNone
        .mapAsync(kBatchSize)(producer.send)
        .groupWithin(pgBatchSize, 100.millis)
        .evalMap {
          _.last.traverse_(rep => pgReader.commitSlot(rep.passthrough))
        }

    source.through(sink)
  }

  def readConfig: IO[Configuration] = {
    import fs2.io.file.*
    import fs2.Chunk
    for {
      path    <- configFile
      content <- Files[IO].readAll(Path(path)).chunks.compile.toVector.map(Chunk.concat(_).toByteBuffer)
      json    <- IO.delay(io.circe.jawn.parseByteBuffer(content)).flatMap(IO.fromEither)
      conf    <- IO.delay(json.as[Configuration]).flatMap(IO.fromEither)
    } yield conf
  }

  def newKafkaProducer(c: KafkaConfig): Resource[IO, KafkaProducer[Bytes, Bytes]] = {
    import org.apache.kafka.clients.producer.*
    import org.apache.kafka.common.serialization.ByteArraySerializer

    val create = IO.blocking {
      val props = new util.HashMap[String, java.lang.Object]()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, c.bootstrap.mkString(","))
      props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.valueOf(c.batch_size))
      c.config.foreach { cv =>
        cv.text.foreach { x =>
          logger0.info(s"Setting producer config `${cv.key}` = '${x}'")
          props.put(cv.key, x)
        }
        cv.integer.foreach { x =>
          logger0.info(s"Setting producer config `${cv.key}` = ${x}")
          props.put(cv.key, Integer.valueOf(x))
        }
      }
      new KafkaProducer[Bytes, Bytes](props, new ByteArraySerializer, new ByteArraySerializer)
    }

    Resource.make(create) { p =>
      IO.blocking(p.close(java.time.Duration.ofSeconds(15)))
    }
  }

  def newPgPool(c: PgConfig): Resource[IO, HikariTransactor[IO]] = {
    val poolSize    = c.pool_size.getOrElse(3)
    val maxLifetime = c.max_lifetime_seconds.getOrElse(5L * 60)
    val idleTimeout = c.idle_timeout_seconds.getOrElse(15L)

    for {
      ce   <- ExecutionContexts.fixedThreadPool[IO](poolSize)
      pool <- HikariTransactor.newHikariTransactor[IO](
                driverClassName = "org.postgresql.Driver",
                url             = c.url,
                user            = c.user,
                pass            = c.pass,
                connectEC       = ce
              )
      _    <- Resource.eval {
                pool.configure { h =>
                  IO.delay {
                    h.setMaxLifetime(TimeUnit.SECONDS.toMillis(maxLifetime))
                    h.setIdleTimeout(TimeUnit.SECONDS.toMillis(idleTimeout))
                    h.setMaximumPoolSize(poolSize)
                    h.setMinimumIdle(0)
                  }
                }
              }
    } yield pool
  }
}

@JsonCodec
case class Configuration(
  postgres: PgConfig,
  kafka: KafkaConfig,
  topic: TopicConfig
)

@JsonCodec
case class TopicConfig(
  name: Option[String],
  prefix: Option[String],
  filter: Option[TableFilter]
)

@JsonCodec
case class TableFilter(
  is_whitelist: Boolean,
  tables: Vector[String],
  schema_whitelist: Vector[String]
)

@JsonCodec
case class PgConfig(
  url: String,
  user: String,
  pass: String,
  numeric_as_string: Option[Boolean], // defaults to false
  pool_size: Option[Int],             // defaults to 3
  max_lifetime_seconds: Option[Long], // defaults to 5 minutes
  idle_timeout_seconds: Option[Long], // defaults to 15 seconds
  poll_interval_millis: Option[Long], // defaults to 200 millis
  batch_size: Int,
  batch_buffer: Int,
  slot_name: String,
  data_slot_name: Option[String]      // defaults to {slot_name}_read
)

@JsonCodec
case class KafkaConfig(
  bootstrap: Vector[String],
  batch_size: Int,
  config: Vector[CfgVar]
)

@JsonCodec
case class CfgVar(
  key: String,
  integer: Option[Int],
  text: Option[String]
)
