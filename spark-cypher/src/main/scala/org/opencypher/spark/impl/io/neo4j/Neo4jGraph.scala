/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.impl.io.neo4j

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.neo4j.driver.internal.{InternalNode, InternalRelationship}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.PropertyKey
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.io.neo4j.Neo4jGraph.{filterNode, filterRel, nodeToRow, relToRow}
import org.opencypher.spark.impl.table.CAPSRecordHeader
import org.opencypher.spark.impl.{CAPSGraph, CAPSRecords}
import org.opencypher.spark.schema.CAPSSchema

class Neo4jGraph(val schema: CAPSSchema, val session: CAPSSession)(
  inputNodes: RDD[InternalNode],
  inputRels: RDD[InternalRelationship],
  sourceNode: String = "source",
  rel: String = "rel",
  targetNode: String = "target")
  extends CAPSGraph {

  override val tags = Set(0)

  protected implicit val caps = session

  private def map(
    f: RDD[InternalNode] => RDD[InternalNode],
    g: RDD[InternalRelationship] => RDD[InternalRelationship]) =
  // We need to construct new RDDs since otherwise providing a different storage level may fail
    new Neo4jGraph(schema, session)(
      f(inputNodes.filter(_ => true)),
      g(inputRels.filter(_ => true)),
      sourceNode,
      rel,
      targetNode)

  override def nodes(name: String, cypherType: CTNode): CAPSRecords = {
    val header = RecordHeader.nodeFromSchema(Var(name)(cypherType), schema)

    computeRecords(name, cypherType, header) { (header, struct) =>
      inputNodes.filter(filterNode(cypherType)).map(nodeToRow(header, struct))
    }
  }

  override def relationships(name: String, cypherType: CTRelationship): CAPSRecords = {
    val header = RecordHeader.relationshipFromSchema(Var(name)(cypherType), schema)

    computeRecords(name, cypherType, header) { (header, struct) =>
      inputRels.filter(filterRel(cypherType)).map(relToRow(header, struct))
    }
  }

  private def computeRecords(name: String, cypherType: CypherType, header: RecordHeader)(
    computeRdd: (RecordHeader, StructType) => RDD[Row]): CAPSRecords = {
    val struct = CAPSRecordHeader.asSparkStructType(header)
    val rdd = computeRdd(header, struct)
    val slot = header.slotFor(Var(name)(cypherType))
    val rawData = session.sparkSession.createDataFrame(rdd, struct)
    val col = rawData.col(rawData.columns(slot.index))
    val recordData = rawData.repartition(col).sortWithinPartitions(col)
    CAPSRecords.verifyAndCreate(header, recordData)(session)
  }

  override def toString = "Neo4jGraph"
}

object Neo4jGraph {

  private case class filterNode(nodeType: CTNode) extends (InternalNode => Boolean) {

    val requiredLabels: Set[String] = nodeType.labels

    override def apply(importedNode: InternalNode): Boolean =
      requiredLabels.forall(importedNode.hasLabel)
  }

  private case class nodeToRow(header: RecordHeader, schema: StructType) extends (InternalNode => Row) {

    override def apply(importedNode: InternalNode): Row = {
      import scala.collection.JavaConverters._

      val props = importedNode.asMap().asScala
      val labels = importedNode.labels().asScala.toSet

      val values = header.slots.map { slot =>
        slot.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = props.get(keyName).orNull
            CypherValue(propValue).unwrap

          case HasLabel(_, label) =>
            labels(label.name)

          case _: Var =>
            importedNode.id()

          case x =>
            throw IllegalArgumentException("a node member expression (property, label, node variable)", x)

        }
      }

      Row(values: _*)
    }
  }

  private case class filterRel(relType: CTRelationship) extends (InternalRelationship => Boolean) {

    override def apply(importedRel: InternalRelationship): Boolean =
      relType.types.isEmpty || relType.types.exists(importedRel.hasType)
  }

  private case class relToRow(header: RecordHeader, schema: StructType) extends (InternalRelationship => Row) {
    override def apply(importedRel: InternalRelationship): Row = {
      import scala.collection.JavaConverters._

      val relType = importedRel.`type`()
      val props = importedRel.asMap().asScala

      val values = header.slots.map { slot =>
        slot.content.key match {
          case Property(_, PropertyKey(keyName)) =>
            val propValue = props.get(keyName).orNull
            CypherValue(propValue).unwrap

          case _: StartNode =>
            importedRel.startNodeId()

          case _: EndNode =>
            importedRel.endNodeId()

          case _: Type =>
            relType

          case _: Var =>
            importedRel.id()

          case x =>
            throw IllegalArgumentException(
              "a relationship member expression (property, start node, end node, type, relationship variable)",
              x)
        }
      }

      Row(values: _*)
    }
  }

}
