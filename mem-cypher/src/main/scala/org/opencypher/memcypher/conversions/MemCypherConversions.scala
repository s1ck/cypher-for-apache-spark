package org.opencypher.memcypher.conversions

import org.opencypher.memcypher.table.{ColumnSchema, MemTableSchema}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object MemCypherConversions {

  implicit class RichRecordHeader(val header: RecordHeader) {
    def toSchema: MemTableSchema = {
      val columnSchemas = header.columns.toSeq.sorted.map { column =>
        val expressions = header.expressionsFor(column)
        val commonType = expressions.map(_.cypherType).reduce(_ join _)
        ColumnSchema(column, commonType)
      }
      MemTableSchema(columnSchemas.toArray)
    }
  }

}
