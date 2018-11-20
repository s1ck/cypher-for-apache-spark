package org.opencypher.memcypher.impl.cyphertable

import org.opencypher.memcypher.impl.records.MemCypherRecordsBehaviour
import org.opencypher.memcypher.impl.table.Table
import org.opencypher.okapi.api.io.conversion.NodeMapping
import org.opencypher.okapi.relational.api.io.NodeTable

case class MemNodeTable(
  override val mapping: NodeMapping,
  override val table: Table
) extends NodeTable(mapping, table) with MemCypherRecordsBehaviour {

  override type Records = MemNodeTable

  override def cache(): MemNodeTable = this
}
