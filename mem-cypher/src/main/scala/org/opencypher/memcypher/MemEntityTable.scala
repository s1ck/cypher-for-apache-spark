package org.opencypher.memcypher

import org.opencypher.okapi.api.io.conversion.EntityMapping
import org.opencypher.okapi.relational.api.io.EntityTable

case class MemEntityTable(mapping: EntityMapping, table: MemTable)
  extends EntityTable[MemTable] with MemCypherRecordsBehaviour {

  override type Records = MemEntityTable

  override def cache(): MemEntityTable = this
}
