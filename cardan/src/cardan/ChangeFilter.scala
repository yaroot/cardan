package cardan

import cats.implicits.*
import KafkaSink.ProduceRecord
import io.circe.{Json, Printer}
import io.circe.generic.JsonCodec
import io.circe.syntax.*

@JsonCodec
case class CapturedValue(
  table: Option[String],
  schema: Option[String],
  timestamp: Option[String],
  columns: Json
)

object CapturedValue {
  implicit val kvSerdes: KVSerdes[CapturedValue] =
    KVSerdes.instance(
      a => Printer.noSpaces.printToByteBuffer(a.asJson).array(),
      xs => io.circe.jawn.decodeByteArray[CapturedValue](xs)
    )
}

trait ChangeFilter {
  type Key         = Json
  type Value       = CapturedValue
  type Passthrough = String

  def pass(x: Record): Option[ProduceRecord[Key, Value, Passthrough]]
}

object ChangeFilter {
  def apply(config: TopicConfig): ChangeFilter = new ChangeFilter {
    require(config.name.isDefined || config.prefix.isDefined)
    require(!(config.name.isDefined && config.prefix.isDefined))

    def topic(schema: String, table: String): String = {
      config.name
        .orElse(config.prefix.map(x => s"$x-$schema-$table"))
        .getOrElse(throw new NotImplementedError("not possible"))
    }

    val filtering: (Record.Change => Boolean) = {
      def hasTable(x: Record.Change) = x.table.nonEmpty && x.schema.nonEmpty
      config.filter match {
        case None                                               => hasTable
        case Some(TableFilter(false, tables, schema_whitelist)) =>
          val validTable  = tables.toSet
          val validSchema = schema_whitelist.toSet
          (x: Record.Change) => {
            hasTable(x)
            && !x.table.exists(validTable)
            && x.schema.exists(validSchema)
          }
        case Some(TableFilter(true, tables, schema_whitelist))  =>
          val validTable  = tables.toSet
          val validSchema = schema_whitelist.toSet
          (x: Record.Change) => {
            hasTable(x)
            && x.table.exists(validTable)
            && x.schema.exists(validSchema)
          }
      }
    }

    def topicFor(x: Record.Change): String = {
      (x.schema, x.table) match {
        case (Some(sch), Some(tab)) => topic(sch, tab)
        case _                      => throw new NotImplementedError("not possible")
      }
    }

    def keyFor(x: Record.Change): Json = {
      def fromPk(pk: Vector[Record.ColumnDef]): Json            = {
        val pkColumnSource =
          x.identity
            .orElse(x.columns)
            .getOrElse(Vector.empty)
        pk.map { key =>
          pkColumnSource
            .find(x => x.name == key.name && x.`type` == key.`type`)
            .map(_.standardize)
        }.asJson
      }
      def fromIdentity(identities: Vector[Record.Column]): Json = {
        identities.map(_.standardize).asJson
      }

      if (x.columns.getOrElse(Vector.empty).nonEmpty)
        x.pk
          .map(fromPk)
          .orElse(x.identity.map(fromIdentity))
          .getOrElse(Json.arr())
      else
        x.identity.map(fromIdentity).getOrElse(Json.arr())
    }

    def valueFor(x: Record.Change): CapturedValue = {
      val columns =
        x.columns.map {
          _.map { c => c.name -> c.standardize }.toMap
        }.asJson

      CapturedValue(
        timestamp = x.timestamp,
        columns   = columns,
        table     = x.table,
        schema    = x.schema
      )
    }

    override def pass(x: Record): Option[ProduceRecord[Key, Value, Passthrough]] = {
      filtering(x.data)
        .guard[Option]
        .map { _ =>
          ProduceRecord(
            topic       = topicFor(x.data),
            key         = keyFor(x.data),
            value       = valueFor(x.data),
            passthrough = x.lsn
          )
        }
    }

  }
}
