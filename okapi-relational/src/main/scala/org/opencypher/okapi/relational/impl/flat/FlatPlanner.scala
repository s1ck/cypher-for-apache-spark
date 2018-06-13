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
package org.opencypher.okapi.relational.impl.flat

import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.types.{CTList, CTNode, CTRelationship, CypherType}
import org.opencypher.okapi.api.value.CypherValue._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.ir.api.util.{DirectCompilationStage, FreshVariableNamer}
import org.opencypher.okapi.logical.impl.LogicalOperator
import org.opencypher.okapi.logical.{impl => logical}
import org.opencypher.okapi.relational.api.schema.RelationalSchema._

import scala.annotation.tailrec

final case class FlatPlannerContext(parameters: CypherMap)

class FlatPlanner extends DirectCompilationStage[LogicalOperator, FlatOperator, FlatPlannerContext] {

  override def process(input: LogicalOperator)(implicit context: FlatPlannerContext): FlatOperator = {
    val producer = new FlatOperatorProducer()

    input match {

      case logical.CartesianProduct(lhs, rhs, _) =>
        producer.cartesianProduct(process(lhs), process(rhs))

      case logical.Select(fields, in, _) =>
        producer.select(fields, process(in))

      case logical.Filter(expr, in, _) =>
        producer.filter(expr, process(in))

      case logical.Distinct(fields, in, _) =>
        producer.distinct(fields, process(in))

      case logical.NodeScan(node, in, _) =>
        val defaultScanVar = Var(PropertyGraph.defaultNodeVarName)(node.cypherType)
        val nodeType = node.cypherType match {
          case ct: CTNode => ct
          case other => throw IllegalArgumentException(CTNode, other)
        }
        producer.planRenameVar(defaultScanVar, node, producer.nodeScan(nodeType, process(in)))

      case logical.Unwind(list, item, in, _) =>
        producer.unwind(list, item, process(in))

      case logical.Project(projectExpr, in, _) =>
        producer.project(projectExpr, process(in))

      case logical.Aggregate(aggregations, group, in, _) =>
        producer.aggregate(aggregations, group, process(in))

      case logical.Expand(source, rel, direction, target, sourceOp, targetOp, _) =>
        producer.expand(source, rel, target, direction, input.graph.schema, process(sourceOp), process(targetOp))

      case logical.ExpandInto(source, rel, target, direction, sourceOp, _) =>
        producer.expandInto(source, rel, target, direction, input.graph.schema, process(sourceOp))

      case logical.ValueJoin(lhs, rhs, predicates, _) =>
        producer.valueJoin(process(lhs), process(rhs), predicates)

      case logical.EmptyRecords(fields, in, _) =>
        producer.planEmptyRecords(fields, process(in))

      case logical.Start(graph, fields, _) =>
        producer.planStart(graph, fields)

      case logical.FromGraph(graph, in, _) =>
        producer.planFromGraph(graph, process(in))

      case logical.BoundedVarLengthExpand(source, edgeList, target, direction, lower, upper, sourceOp, targetOp, _) =>

        def relTypesFromList(t: CypherType): CTRelationship = {
          t match {
            case _@ CTList(inner: CTRelationship) => inner
            case r: CTRelationship => r
            case _ => throw IllegalStateException(s"Required CTList or CTRelationship, but got $t")
          }
        }

        // START
        val prev = producer.planStart(input.graph, Set.empty)
        // INIT VAR EXPAND
        val initVarExpand = producer.initVarExpand(source, edgeList, process(sourceOp))
        // REL SCAN
        val relType = relTypesFromList(edgeList.cypherType)
        val defaultRelScanVar = Var(PropertyGraph.defaultRelVarName)(relType)
        val edgeScan = RelationshipScan(relType, prev, prev.sourceGraph.schema.defaultHeaderForRelType(relType))
        // RENAME REL SCAN
        val customRelScanVar = FreshVariableNamer(edgeList.name + "extended", relType)
        val renamedScan = producer.planRenameVar(defaultRelScanVar, customRelScanVar, edgeScan)

        producer.boundedVarExpand(
          customRelScanVar,
          edgeList,
          target,
          direction,
          lower,
          upper,
          initVarExpand,
          renamedScan,
          process(targetOp),
          isExpandInto = sourceOp == targetOp)

      case logical.Optional(lhs, rhs, _) =>
        producer.planOptional(process(lhs), process(rhs))

      case logical.ExistsSubQuery(expr, lhs, rhs, _) =>
        producer.planExistsSubQuery(expr, process(lhs), process(rhs))

      case logical.OrderBy(sortListItems, sourceOp, _) =>
        producer.orderBy(sortListItems, process(sourceOp))

      case logical.Skip(expr, sourceOp, _) =>
        producer.skip(expr, process(sourceOp))

      case logical.Limit(expr, sourceOp, _) =>
        producer.limit(expr, process(sourceOp))

      case logical.ReturnGraph(in, _) =>
        producer.returnGraph(process(in))

      case x =>
        throw NotImplementedException(s"Flat planning not implemented for $x")
    }


  }
}
