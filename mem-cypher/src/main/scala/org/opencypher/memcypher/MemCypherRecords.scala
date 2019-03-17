package org.opencypher.memcypher

import org.opencypher.memcypher.conversions.MemCypherConversions._
import org.opencypher.memcypher.conversions.MemRowToCypherMap
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.relational.api.io.EntityTable
import org.opencypher.okapi.relational.api.table.{RelationalCypherRecords, RelationalCypherRecordsFactory}
import org.opencypher.okapi.relational.impl.table.RecordHeader


case class MemCypherRecordsFactory()(implicit val session: MemCypherSession)
  extends RelationalCypherRecordsFactory[MemTable] {

  override type Records = MemCypherRecords

  override def unit(): MemCypherRecords =
    MemCypherRecords(RecordHeader.empty, MemTable.unit)

  override def empty(initialHeader: RecordHeader): MemCypherRecords =
    MemCypherRecords(initialHeader, MemTable(initialHeader.toSchema, Seq.empty))

  override def fromEntityTable(entityTable: EntityTable[MemTable]): MemCypherRecords =
    MemCypherRecords(entityTable.header, entityTable.table)

  override def from(
    header: RecordHeader,
    table: MemTable,
    maybeLogicalColumns: Option[Seq[String]]
  ): MemCypherRecords = {
    val logicalColumns = maybeLogicalColumns match {
      case s@Some(_) => s
      case None => Some(header.vars.map(_.withoutType).toSeq)
    }
    MemCypherRecords(header, table, logicalColumns)
  }
}


case class MemCypherRecords(
  header: RecordHeader,
  table: MemTable,
  override val logicalColumns: Option[Seq[String]] = None
) extends RelationalCypherRecords[MemTable] with MemCypherRecordsBehaviour {

  override type Records = MemCypherRecords

  override def cache(): MemCypherRecords = this
}

trait MemCypherRecordsBehaviour extends RelationalCypherRecords[MemTable] {

  override type Records <: MemCypherRecordsBehaviour

  override lazy val columnType: Map[String, CypherType] =
    table.schema.columns.map(columnMeta => columnMeta.name -> columnMeta.dataType).toMap

  override def rows: Iterator[String => CypherValue] =
    iterator.map(_.value)

  override def iterator: Iterator[CypherMap] =
    toCypherMaps.iterator

  override def collect: Array[CypherMap] =
    toCypherMaps.toArray

  def toCypherMaps: Seq[CypherMap] =
    table.data.map(MemRowToCypherMap(header, table.schema))
}
