package cardan

import cats.implicits.*
import cats.effect.*
import io.circe.generic.JsonCodec
import fs2.{Chunk, Pipe, Stream}
import io.circe.Json
import org.apache.kafka.clients.producer.KafkaProducer

import java.util
import scala.concurrent.duration.*

object Main extends IOApp.Simple {
  implicit val loggerFactory: LoggerFactory[IO] = LoggerFactory.default[IO]
  val logger0                                   = org.slf4j.LoggerFactory.getLogger("main")

  override def run: IO[Unit] = {
    val fx = for {
      config       <- Stream.eval(readConfig)
      javaProducer <- Stream.resource(newKafkaProducer(config.kafka))
      pgConn        = PgStreamer.makePostgres[IO](config.postgres)
      rawStream    <- Stream.resource(PgStreamer.makeLogicalStream(config.postgres, pgConn))
      pgStreamer    = PgStreamer[IO](
                        rawStream,
                        config.postgres.batch_size,
                        config.postgres.poll_interval_millis.getOrElse(10).millis
                      )
      changeFilter  = ChangeFilter(config.topic)
      sink          = KafkaSink[IO, Json, CapturedValue, Record.LSN](javaProducer)
      _            <- flow(pgStreamer, sink, changeFilter, config)
    } yield ()

    fx.compile.drain
  }

  def configFile: IO[String] = IO.envForIO.get("CONFIG_FILE").map(_.getOrElse("./app.json"))

  def flow(
    pgStreamer: PgStreamer[IO],
    producer: KafkaSink[IO, Json, CapturedValue, Record.LSN],
    changeFilter: ChangeFilter,
    config: Configuration
  ): Stream[IO, Unit] = {
    val pgBatchSize = config.postgres.batch_buffer * config.postgres.batch_size
    val kBatchConc  = (config.kafka.batch_size / pgBatchSize).max(5)

    val source: Stream[IO, Chunk[Record]] = Stream
      .eval(pgStreamer.readBatch)
      .repeat
      .flatMap(Stream.emits)
      .groupWithin(pgBatchSize, 50.millis)

    val sink: Pipe[IO, Chunk[Record], Unit] =
      _.map(_.map(changeFilter.pass).unite)
        .filter(_.nonEmpty)
        .mapAsync(kBatchConc)(chunk => producer.sendBatch(chunk.toVector))
        .map(_.lastOption.map(_.passthrough))
        .groupWithin(kBatchConc, 50.millis)
        .evalMap {
          _.last.flatten.traverse_(pgStreamer.commit)
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
  numeric_as_string: Option[Boolean],  // defaults to false
  poll_interval_millis: Option[Int],   // defaults to 10 millis
  commit_interval_millis: Option[Int], // defaults to 1000 millis
  recv_buffer_size: Option[Int],       // defaults to 512k
  batch_size: Int,
  batch_buffer: Int,
  slot_name: String
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
