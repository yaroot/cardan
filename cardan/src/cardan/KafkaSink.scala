package cardan

import cats.implicits.*
import cats.effect.*
import io.circe.{Json, Printer}
import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}

import java.nio.charset.StandardCharsets
import scala.concurrent.{Future, Promise}
import scala.util.Try

trait KafkaSink[F[_], K, V, P] {
  import KafkaSink.{ProduceRecord, Sent}
  type Rec = ProduceRecord[K, V, P]
  type Rep = Sent[P]

  def send(x: Rec): F[Rep]
  def sendBatch(xs: Vector[Rec]): F[Vector[Rep]]
}

trait KVSerdes[A] {
  def serialize(a: A): Bytes
  def deserialize(xs: Bytes): Either[Throwable, A]
}

object KVSerdes {
  def apply[A: KVSerdes]: KVSerdes[A] = implicitly

  def instance[A](ser: A => Bytes, des: Bytes => Either[Throwable, A]): KVSerdes[A] =
    new KVSerdes[A] {
      override def serialize(a: A): Bytes                       = ser(a)
      override def deserialize(xs: Bytes): Either[Throwable, A] = des(xs)
    }

  implicit val stringSerdes: KVSerdes[String] =
    KVSerdes.instance(
      _.getBytes(StandardCharsets.UTF_8),
      xs => Try(new String(xs, StandardCharsets.UTF_8)).toEither
    )

  implicit val jsonSerdes: KVSerdes[Json] =
    KVSerdes.instance(
      Printer.noSpaces.printToByteBuffer(_).array(),
      io.circe.jawn.parseByteArray
    )
}

object KafkaSink {
  case class Sent[P](passthrough: P, meta: RecordMetadata)
  case class ProduceRecord[K, V, P](topic: String, key: K, value: V, passthrough: P)

  def apply[F[_]: Async: LoggerFactory, K: KVSerdes, V: KVSerdes, P](
    producer: Producer[Bytes, Bytes]
  ): KafkaSink[F, K, V, P] =
    new KafkaSink[F, K, V, P] {
      val logger = LoggerFactory[F].getLogger("KafkaSink")

      def send(x: Rec): F[Rep] = {
        val fx = Async[F].delay(unsafeSend(x))
        Async[F].fromFuture(fx)
      }

      def sendBatch(xs: Vector[Rec]): F[Vector[Rep]] = {
        logger.info(s"Sending ${xs.size} msgs") >>
          Async[F].executionContext.flatMap { implicit ec =>
            Async[F].fromFuture {
              Async[F].delay {
                Future.traverse(xs)(unsafeSend)
              }
            }
          }
      }

      def unsafeSend(x: Rec): Future[Rep] = {
        val p = Promise[Rep]()
        producer.send(
          new ProducerRecord[Bytes, Bytes](
            x.topic,
            KVSerdes[K].serialize(x.key),
            KVSerdes[V].serialize(x.value)
          ),
          new Callback {
            override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
              if (exception eq null) p.success(Sent(x.passthrough, metadata))
              else p.failure(exception)
            }
          }
        )
        p.future
      }
    }
}
