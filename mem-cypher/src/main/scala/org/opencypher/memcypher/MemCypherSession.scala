package org.opencypher.memcypher

import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraphFactory, RelationalCypherSession}

case class MemCypherSession() extends RelationalCypherSession[MemTable] {

  private implicit val _session: MemCypherSession = this

  override type Records = MemCypherRecords

  override private[opencypher] def records: MemCypherRecordsFactory = MemCypherRecordsFactory()

  override private[opencypher] def graphs: RelationalCypherGraphFactory[MemTable] =
    new RelationalCypherGraphFactory[MemTable] {
      override implicit val session: RelationalCypherSession[MemTable] = _session
    }

  override private[opencypher] def entityTables = ???
}
