package cardan

import cats.effect.*
import cats.implicits.*
import io.circe
import org.postgresql.jdbc.PgConnection
import org.postgresql.replication.{LogSequenceNumber, PGReplicationStream}
import io.circe.jawn as Jawn
import org.postgresql.PGProperty

import java.nio.ByteBuffer
import java.sql.DriverManager
import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.annotation.tailrec
import scala.concurrent.duration.FiniteDuration
import scala.util.Try

trait PgStreamer[F[_]] {
  def readBatch: F[Vector[Record]]
  def commit(lsn: LogSequenceNumber): F[Unit]
}

object PgStreamer {
  def apply[F[_]: Async: LoggerFactory](
    pgLogicalStream: PGReplicationStream,
    readBatchSize: Int,
    sleepInterval: FiniteDuration
  ): PgStreamer[F] = new PgStreamer[F] {
    val logger = LoggerFactory[F].getLogger("PgStreamer")

    def unsafeRead(): Option[(ByteBuffer, LogSequenceNumber)] = {
      Option(pgLogicalStream.readPending())
        .map(_ -> pgLogicalStream.getLastReceiveLSN)
    }

    def unsafeReadBatch(max: Int): Vector[(ByteBuffer, LogSequenceNumber)] = {
      val buffer                    = Vector.newBuilder[(ByteBuffer, LogSequenceNumber)]
      @tailrec def go(n: Int): Unit = {
        if (n >= max) ()
        else {
          unsafeRead() match {
            case Some(x) =>
              buffer += x
              go(n + 1)
            case _       => ()
          }
        }
      }
      go(0)
      buffer.result()
    }

    override def readBatch: F[Vector[Record]] = {
      def decodeChange(bb: ByteBuffer): Either[circe.Error, Record.Change] = Jawn.decodeByteBuffer[Record.Change](bb)

      unsafeBlock(unsafeReadBatch(readBatchSize))
        .flatTap { xs =>
          if (xs.isEmpty) Async[F].sleep(sleepInterval) else ().pure[F]
        }
        .flatMap {
          _.traverse { case (buf, lsn) =>
            decodeChange(buf).map(Record(lsn, _))
          }
            .liftTo[F]
        }
    }

    override def commit(lsn: LogSequenceNumber): F[Unit] = {
      logger.info(s"Committing ${lsn.asString()}") >>
        unsafeBlock {
          pgLogicalStream.setFlushedLSN(lsn)
          pgLogicalStream.setAppliedLSN(lsn)
        }
    }
  }

  def unsafeBlock[F[_]: Sync, A](thunk: => A): F[A] = {
    Sync[F].blocking {
      Try { thunk }.liftTo[F]
    }.flatten
  }

  def makePostgres[F[_]: Sync](pgConfig: PgConfig): Resource[F, PgConnection] = {
    val url   = pgConfig.url
    val props = new Properties()
    PGProperty.USER.set(props, pgConfig.user)
    PGProperty.PASSWORD.set(props, pgConfig.pass)
    PGProperty.REPLICATION.set(props, "database")
    PGProperty.ASSUME_MIN_SERVER_VERSION.set(props, "9.4")
    PGProperty.RECEIVE_BUFFER_SIZE.set(props, pgConfig.recv_buffer_size.getOrElse(512 * 1024))

    val create = unsafeBlock {
      locally { Class.forName("org.postgresql.Driver") }
      DriverManager.getConnection(url, props)
    }

    Resource
      .make(create)(conn => unsafeBlock(conn.close()))
      .map(_.unwrap(classOf[PgConnection]))
  }

  def makeLogicalStream[F[_]: Async](
    pgConfig: PgConfig,
    pgConnection: Resource[F, PgConnection]
  ): Resource[F, PGReplicationStream] = {
    pgConnection
      .flatMap { connection =>
        val create =
          unsafeBlock {
            val settings = List(
              "format-version"    -> "2",
              "include-timestamp" -> "true",
              "include-types"     -> "true",
              "include-pk"        -> "true"
            ) ++ (
              pgConfig.numeric_as_string
                .filter(identity)
                .map(_ => "numeric-data-types-as-string" -> "true")
                .toList
            )

            val props = new Properties()
            settings.foreach { case (key, value) =>
              props.setProperty(key, value)
            }

            val slotName = s"\"${pgConfig.slot_name}\""

            connection.getReplicationAPI
              .replicationStream()
              .logical()
              .withSlotOptions(props)
              .withSlotName(slotName)
              .withStatusInterval(pgConfig.commit_interval_millis.getOrElse(1000), TimeUnit.MILLISECONDS)
              .start()
          }
        Resource.make(create)(x => unsafeBlock(x.close()))
      }
  }

}
