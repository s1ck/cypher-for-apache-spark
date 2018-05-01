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
package org.opencypher.spark.impl

import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.okapi.api.types.{CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.GraphEntity
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.convert.CAPSCypherType._
import org.opencypher.spark.schema.CAPSSchema

abstract class CAPSCanonicalLazyScanGraph(val schema: CAPSSchema, val tags: Set[Int])
  (implicit val session: CAPSSession) extends CAPSGraph {

  self: CAPSGraph =>

  def nodeScan(combo: Set[String]): DataFrame

  def relScan(relType: String): DataFrame

  override def toString = s"CAPSScanGraph(${schema.pretty})"

  override def nodes(name: String, nodeCypherType: CTNode): CAPSRecords = nodesInternal(name, nodeCypherType, byExactType = false)

  override def nodesWithExactLabels(name: String, labels: Set[String]): CAPSRecords = nodesInternal(name, CTNode(labels), byExactType = true)

  private def nodesInternal(name: String, nodeCypherType: CTNode, byExactType: Boolean): CAPSRecords = {
    val labels = nodeCypherType.labels
    if (byExactType) {
      nodeScan(nodeCypherType.labels).toNodeTable(name, labels, Set.empty)
    } else {
      val combos = schema.combinationsFor(nodeCypherType.labels)
      val allLabels = combos.flatten
      val propertyKeys = schema.keysFor(Set(nodeCypherType.labels)).keySet
      val optionalProperties: Map[String, CypherType] = schema.keysFor(combos) -- propertyKeys
      val trueLit = functions.lit(true)
      val falseLit = functions.lit(false)
      val nullLit = functions.lit(null)

      val tablesWithAllLabels = combos.map { combo =>
        val tableWithoutExtraLabels = nodeScan(combo)
        val existingLabels = tableWithoutExtraLabels.columns.toSet
        val trueExtraLabels = (allLabels -- existingLabels).intersect(combo)
        val tableWithTrueExtraLabels = tableWithoutExtraLabels.safeAddColumns(trueExtraLabels.toSeq.map(_ -> trueLit): _*)
        val falseExtraLabels = allLabels -- existingLabels -- trueExtraLabels
        val tableWithAllExtraLabels = tableWithTrueExtraLabels.safeAddColumns(falseExtraLabels.toSeq.map(_ -> falseLit): _*)
        tableWithAllExtraLabels
      }.toSeq

      val tablesWithOptionalProperties = tablesWithAllLabels.map { table =>
        val propertiesToAddNullFor = optionalProperties -- table.columns.toSet
        propertiesToAddNullFor.foldLeft(table) { case (tmpTable, (propertyKey, propertyType)) =>
          tmpTable.safeAddColumn(propertyKey, nullLit.cast(propertyType.getSparkType))
        }
      }

      // TODO: Remove once we have access to Spark 2.3 `unionByName`: https://issues.apache.org/jira/browse/SPARK-21043
      val alignedTables = tablesWithOptionalProperties.map { table =>
        val labelAndPropertyColumns: Seq[String] = allLabels.toSeq.sorted ++ propertyKeys.toSeq.sorted ++ optionalProperties.keys.toSeq.sorted
        table.select(GraphEntity.sourceIdKey, labelAndPropertyColumns: _*)
      }

      // TODO: Bug with mapping/planner related to using implied labels from the var. Working around this by making all labels optional.
      // Specifying the mandatory labels without adding columns for them should work, but doesn't.
      alignedTables.tail.foldLeft(alignedTables.head)(_.union(_)).toNodeTable(name, Set.empty[String], allLabels)
    }
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPSRecords = {
    val relTypes = relCypherType.types.toSeq
    if (relTypes.size == 1) {
      val relType = relTypes.head
      relScan(relType).toRelTable(name, relType)
    } else {
      // TODO: Replace with cheaper alignment similar to what is done with the node tables
      val rel = Var(name)(relCypherType)
      val selectedRelTypes = if (relTypes.isEmpty) schema.relationshipTypes else relCypherType.types
      val relTables: Set[CAPSRecords] = selectedRelTypes.map(relType => relScan(relType).toRelTable(name, relType))
      val targetRelHeader = RecordHeader.relationshipFromSchema(rel, schema)
      val alignedRecords = relTables.map(_.alignWith(rel, targetRelHeader))
      alignedRecords.reduceOption(_ unionAll(targetRelHeader, _)).getOrElse(CAPSRecords.empty(targetRelHeader))
    }
  }

}
