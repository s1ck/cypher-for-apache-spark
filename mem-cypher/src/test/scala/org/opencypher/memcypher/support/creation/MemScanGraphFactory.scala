package org.opencypher.memcypher.support.creation

import org.opencypher.memcypher.{MemCypherSession, MemEntityTable, MemTable}
import org.opencypher.memcypher.table.{ColumnSchema, MemTableRow, MemTableSchema}
import org.opencypher.okapi.api.io.conversion.{NodeMappingBuilder, RelationshipMappingBuilder}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.types.CTInteger
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.okapi.testing.propertygraph.{CypherTestGraphFactory, InMemoryTestGraph}

object MemScanGraphFactory extends CypherTestGraphFactory[MemCypherSession] {

  private val tableEntityIdKey = "___id"
  private val tableEntityStartNodeKey = "___source"
  private val tableEntityEndNodeKey = "___target"

  override def name: String = "MemScanGraphFactory"

  override def apply(propertyGraph: InMemoryTestGraph)(implicit session: MemCypherSession): ScanGraph[MemTable] = {
    val graphSchema = computeSchema(propertyGraph)

    val nodeScans = graphSchema.labelCombinations.combos.map { labels =>
      val propKeys = graphSchema.nodePropertyKeys(labels)

      val idColumnSchema = Array(ColumnSchema(tableEntityIdKey, CTInteger))
      val tableSchema = MemTableSchema(idColumnSchema ++ getPropertyColumnSchemas(propKeys))

      val rows = propertyGraph.nodes
        .filter(_.labels == labels)
        .map { node =>
          val propertyValues = propKeys.map(key =>
            node.properties.unwrap.getOrElse(key._1, null)
          )
          MemTableRow.fromSeq(Seq(node.id) ++ propertyValues)
        }

      MemEntityTable(NodeMappingBuilder
        .on(tableEntityIdKey)
        .withImpliedLabels(labels.toSeq: _*)
        .withPropertyKeys(propKeys.keys.toSeq: _*)
        .build, MemTable(tableSchema, rows))
    }

    val relScans = graphSchema.relationshipTypes.map { relType =>
      val propKeys = graphSchema.relationshipPropertyKeys(relType)

      val idColumnSchemas = Array(
        ColumnSchema(tableEntityIdKey, CTInteger),
        ColumnSchema(tableEntityStartNodeKey, CTInteger),
        ColumnSchema(tableEntityEndNodeKey, CTInteger))
      val tableSchema = MemTableSchema(idColumnSchemas ++ getPropertyColumnSchemas(propKeys))

      val rows = propertyGraph.relationships
        .filter(_.relType == relType)
        .map { rel =>
          val propertyValues = propKeys.map(key => rel.properties.unwrap.getOrElse(key._1, null))
          MemTableRow.fromSeq(Seq(rel.id, rel.startId, rel.endId) ++ propertyValues)
        }

      MemEntityTable(RelationshipMappingBuilder
        .on(tableEntityIdKey)
        .from(tableEntityStartNodeKey)
        .to(tableEntityEndNodeKey)
        .relType(relType)
        .withPropertyKeys(propKeys.keys.toSeq: _*)
        .build, MemTable(tableSchema, rows))
    }

    val allTables = nodeScans.toSeq ++ relScans
    session.graphs.create(allTables.head, allTables.tail: _*)
    new ScanGraph(nodeScans.toSeq ++ relScans, graphSchema)
  }

  protected def getPropertyColumnSchemas(propKeys: PropertyKeys): Seq[ColumnSchema] = {
    propKeys.foldLeft(Seq.empty[ColumnSchema]) {
      case (fields, key) => fields :+ ColumnSchema(key._1, key._2)
    }
  }
}
