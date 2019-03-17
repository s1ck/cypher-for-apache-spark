package org.opencypher.memcypher

import org.opencypher.memcypher.table.{MemTableRow, MemTableSchema}
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.types.{CTIdentity, CTInteger, CTString}
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan

object Demo extends App {

  implicit val session: MemCypherSession = MemCypherSession()

  val graph = session.readFrom(DemoData.nodes, DemoData.rels)
  PrintRelationalPlan.set()
    graph.cypher("MATCH (n)-->(m) WHERE n.age > 23 OR n.name = 'Alice' RETURN n, labels(n), m").show
  //  graph.cypher("MATCH (n) RETURN n, n.name CONTAINS 'A' ORDER BY n.age ASC, n.name DESC").show
  //  graph.cypher("MATCH (n) RETURN n.gender, count(DISTINCT n.name), min(n.age), max(n.age), avg(n.age)").show
  //  graph.cypher("MATCH (n) RETURN count(*)").show

}

object DemoData {

  def nodes: MemEntityTable = {
    val schema = MemTableSchema.empty
      .withColumn("id", CTIdentity)
      .withColumn("age", CTInteger)
      .withColumn("name", CTString)
      .withColumn("gender", CTString)

    val data = Seq(
      MemTableRow(0L, 23L, "Alice", "w"),
      MemTableRow(1L, 42L, "Bob", "m"),
      MemTableRow(2L, 42L, "Eve", "w"),
      MemTableRow(3L, 84L, "Frank", "m")
    )

    val nodeMapping = NodeMappingBuilder.on("id")
      .withImpliedLabel("Person")
      .withImpliedLabel("Human")
      .withPropertyKey("age")
      .withPropertyKey("name")
      .withPropertyKey("gender")
      .build

    MemEntityTable(nodeMapping, MemTable(schema, data))
  }

  def rels: MemEntityTable = {
    val schema = MemTableSchema.empty
      .withColumn("id", CTIdentity)
      .withColumn("source", CTIdentity)
      .withColumn("target", CTIdentity)
      .withColumn("since", CTString)

    val data = Seq(MemTableRow(0L, 0L, 1L, 1984L))

    val relMapping = RelationshipMappingBuilder.on("id")
      .from("source")
      .to("target")
      .withRelType("KNOWS")
      .withPropertyKey("since")
      .build

    MemEntityTable(relMapping, MemTable(schema, data))
  }
}
