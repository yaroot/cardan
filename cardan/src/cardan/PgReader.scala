package cardan

import cats.implicits.*
import cats.effect.*
import doobie.{ConnectionIO, Transactor}
import doobie.syntax.all.*
import doobie.postgres.circe.jsonb.implicits.*
import io.circe.Json
import io.circe.generic.JsonCodec
import io.circe.syntax.*
import io.circe.Printer
import io.circe.jawn as Jawn
import fs2.Stream

import scala.concurrent.duration.*
import scala.util.chaining.*

@JsonCodec
case class Record(lsn: String, data: Record.Change)
object Record {
  implicit val kvSerdes: KVSerdes[Record] =
    KVSerdes.instance(
      a => Printer.noSpaces.printToByteBuffer(a.asJson).array(),
      Jawn.decodeByteArray[Record](_)
    )

  @JsonCodec
  case class Change(
    action: String, // U, I, B, C, D, M
    schema: Option[String],
    table: Option[String],
    timestamp: Option[String],
    identity: Option[Vector[Column]],
    columns: Option[Vector[Column]],
    pk: Option[Vector[ColumnDef]]
  )

  implicit val changeGet: doobie.Get[Change] = pgDecoderGetT[Change]
  implicit val changePut: doobie.Put[Change] = pgEncoderPutT[Change]

  @JsonCodec
  case class ColumnDef(name: String, `type`: String)

  @JsonCodec
  case class Column(
    name: String,
    `type`: String,
    value: Json
  ) {
    @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
    def standardize: Json = {
      `type` match {
        case "bytea" if value.isString => ("\\x" + value.asString.get).asJson // safe
        case "json" | "jsonb"          => unsafeParseNestedJsonString(value)  // also safe
        case _                         => value
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def unsafeParseNestedJsonString(value: Json): Json = {
    (value.asString.get)
      .pipe(Jawn.parse(_).fold(throw _, identity))
  }
}

trait PgReader[F[_]] {
  def resetDataSlot: F[Unit]
  def readBatch: Stream[F, Record]
  def commitSlot(lsn: String): F[Unit]
}

object PgReader {

  def apply[F[_]: PgReader]: PgReader[F] = implicitly

  def apply[F[_]: Temporal: LoggerFactory](config: PgConfig, xa: Transactor[F]): PgReader[F] = new PgReader[F] {
    val logger = LoggerFactory[F].getLogger("pgreader")

    val pollIntervalMs: FiniteDuration = config.poll_interval_millis.getOrElse(200L).millis
    val logicalSlot: String            = config.slot_name
    val dataSlot: String               = config.data_slot_name.getOrElse(s"${config.slot_name}_read")

    override def resetDataSlot: F[Unit] = {
      logger.info("Resetting data slot")
        >> slotExists(dataSlot)
          .flatMap(_.traverse_(_ => dropSlot(dataSlot)))
          .transact(xa)
        >> copySlot(logicalSlot, dataSlot).transact(xa)
    }

    val readSql: doobie.Fragment = {
      val numericAsString =
        if (config.numeric_as_string.exists(identity))
          fr0" 'numeric-data-types-as-string', 'true', "
        else
          fr0""

      (
        sql"""
          select lsn::text, data :: jsonb from
            pg_logical_slot_get_changes(
              ${dataSlot} :: text,
              null :: pg_lsn,
              ${config.batch_size} :: integer,
              'include-timestamp', 'true',
              'include-types', 'true',
              'include-pk', 'true',
          """
          ++ numericAsString
          ++ fr0" 'format-version', '2') "
      )
    }

    override def readBatch: Stream[F, Record] = {
      val poll = readSql
        .query[Record]
        .to[Vector]
        .transact(xa)
        .flatTap { xs =>
          if (xs.isEmpty) Temporal[F].sleep(pollIntervalMs)
          else ().pure[F]
        }

      val pollStream = Stream.eval(poll).flatMap(Stream.emits)

      Stream.eval(logger.trace("read batch")) >> pollStream
    }

    override def commitSlot(lsn: String): F[Unit] = {
      // pg_replication_slot_advance doesn't work
      def go: ConnectionIO[Unit] =
        advanceSlot(logicalSlot, lsn)
          .flatMap {
            case true  => ().pure[ConnectionIO]
            case false => go
          }

      logger.info(s"Commit ${lsn}") >>
        go.transact(xa)
    }

    def advanceSlot(slotName: String, lsn: String): ConnectionIO[Boolean] = {
      sql"""
          select lsn::text
          from pg_logical_slot_get_changes(
                 $slotName :: text,
                 $lsn :: pg_lsn,
                 ${config.batch_size * 10} :: integer,
                 'format-version', '2'
               )
      """
        .query[String]
        .to[Vector]
        .map(_.lastOption.forall(_ == lsn))
    }

    def dropSlot(slotName: String): ConnectionIO[Unit] = {
      sql"select 'drop' from pg_drop_replication_slot($slotName :: text)"
        .query[String]
        .unique
        .void
    }

    def slotExists(slotName: String): ConnectionIO[Option[(String, String)]] = {
      sql"select slot_name, confirmed_flush_lsn :: text from pg_replication_slots where slot_name = ${slotName} :: text and database = current_database()"
        .query[(String, String)]
        .option
    }

    def copySlot(srcName: String, destName: String): ConnectionIO[Unit] = {
      sql"select 'init' from pg_copy_logical_replication_slot($srcName :: text, $destName :: text)"
        .query[String]
        .unique
        .void
    }
  }

}
