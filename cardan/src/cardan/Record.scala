package cardan

import doobie.postgres.circe.jsonb.implicits.*
import io.circe.{Decoder, Encoder, Json}
import io.circe.generic.JsonCodec
import io.circe.syntax.*
import io.circe.Printer
import io.circe.jawn as Jawn
import org.postgresql.replication.LogSequenceNumber

import scala.util.Try
import scala.util.chaining.*

@JsonCodec
case class Record(lsn: Record.LSN, data: Record.Change)
object Record {
  type LSN = LogSequenceNumber

  implicit val lsnEncoder: Encoder[Record.LSN] = Encoder[String].contramap(_.asString())
  implicit val lsnDecoder: Decoder[Record.LSN] = Decoder[String].emapTry(a => Try(LogSequenceNumber.valueOf(a)))

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
        case _ if value.isNull         => value                              // null
        case "json" | "jsonb"          => unsafeParseNestedJsonString(value) // also safe
        case "bytea" if value.isString =>
          ("\\x" + value.asString.getOrElse(raise("empty bytea value"))).asJson // safe
        case _                         => value
      }
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def unsafeParseNestedJsonString(value: Json): Json = {
    value.asString
      .getOrElse(raise("json/jsonb value is not string encoded"))
      .pipe(Jawn.parse(_).fold(throw _, identity))
  }

  @inline
  def raise(msg: String): Nothing = throw new RuntimeException(msg)
}
