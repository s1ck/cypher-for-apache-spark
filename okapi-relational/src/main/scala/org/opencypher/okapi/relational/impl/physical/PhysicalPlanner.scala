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
package org.opencypher.okapi.relational.impl.physical

import org.opencypher.okapi.api.graph.{CypherSession, PropertyGraph}
import org.opencypher.okapi.api.table.CypherRecords
import org.opencypher.okapi.api.types.{CTBoolean, CTNode}
import org.opencypher.okapi.impl.exception.NotImplementedException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.physical.{PhysicalOperator, PhysicalOperatorProducer, PhysicalPlannerContext, RuntimeContext}
import org.opencypher.okapi.relational.impl.flat
import org.opencypher.okapi.relational.impl.flat.FlatOperator
import org.opencypher.okapi.relational.impl.table._

class PhysicalPlanner[P <: PhysicalOperator[R, G, C], R <: CypherRecords, G <: PropertyGraph, C <: RuntimeContext[R, G]](
  producer: PhysicalOperatorProducer[P, R, G, C]
)

  extends DirectCompilationStage[FlatOperator, P, PhysicalPlannerContext[P, R]] {

  def process(flatPlan: FlatOperator)(implicit context: PhysicalPlannerContext[P, R]): P = {

    implicit val caps: CypherSession = context.session

    flatPlan match {
      case flat.CartesianProduct(lhs, rhs, header) =>
        producer.planCartesianProduct(process(lhs), process(rhs), header)

      case flat.Select(fields, in, header) =>

        val selectExpressions = fields
          .flatMap(header.ownedBy)
          .distinct

        producer.planSelect(process(in), selectExpressions.map(_ -> None), header)

      case flat.EmptyRecords(in, header) =>
        producer.planEmptyRecords(process(in), header)

      case flat.Start(graph, _) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planStart(Some(g.qualifiedGraphName), Some(context.inputRecords))
          case p: LogicalPatternGraph =>
            context.constructedGraphPlans.get(p.name) match {
              case Some(plan) => plan // the graph was already constructed
              case None => planConstructGraph(None, p) // plan starts with a construct graph, thus we have to plan it
            }
        }

      case flat.FromGraph(graph, in) =>
        graph match {
          case g: LogicalCatalogGraph =>
            producer.planFromGraph(process(in), g)

          case construct: LogicalPatternGraph =>
            planConstructGraph(Some(in), construct)
        }

      case op@flat.NodeScan(nodeType, in, header) =>
        producer.planNodeScan(process(in), op.sourceGraph, nodeType, header)

      case op@flat.RelationshipScan(relType, in, header) =>
        producer.planRelationshipScan(process(in), op.sourceGraph, relType, header)

      case flat.Unwind(explodeExpr: Explode, item, in, header) =>
        producer.planProject(process(in), explodeExpr, Some(item), header)

      case flat.Project(expr, alias, in, header) =>
        producer.planProject(process(in), expr, alias, header)

      case flat.Aggregate(aggregations, group, in, header) => producer.planAggregate(process(in), group, aggregations, header)

      case flat.Filter(expr, in, header) => expr match {
        case TrueLit() =>
          process(in) // optimise away filter
        case _ =>
          producer.planFilter(process(in), expr, header)
      }

      case flat.ValueJoin(lhs, rhs, predicates, header) =>
        val joinExpressions = predicates.map(p => p.lhs -> p.rhs).toSeq
        producer.planJoin(process(lhs), process(rhs), joinExpressions, header)

      case flat.Distinct(fields, in, _) =>
        producer.planDistinct(process(in), fields)

      // TODO: This needs to be a ternary operator taking source, rels and target records instead of just source and target and planning rels only at the physical layer
      case op@flat.Expand(source, rel, direction, target, sourceOp, targetOp, header, relHeader) =>
        val first = process(sourceOp)
        val third = process(targetOp)

        val startFrom = sourceOp.sourceGraph match {
          case e: LogicalCatalogGraph =>
            producer.planStart(Some(e.qualifiedGraphName))

          case c: LogicalPatternGraph =>
            context.constructedGraphPlans(c.name)
        }

        val second = producer.planRelationshipScan(startFrom, op.sourceGraph, rel, relHeader)
        val startNode = StartNode(rel)(CTNode)
        val endNode = EndNode(rel)(CTNode)

        direction match {
          case Directed =>
            val tempResult = producer.planJoin(first, second, Seq(source -> startNode), first.header ++ second.header)
            producer.planJoin(tempResult, third, Seq(endNode -> target), header)

          case Undirected =>
            val tempOutgoing = producer.planJoin(first, second, Seq(source -> startNode), first.header ++ second.header)
            val outgoing = producer.planJoin(tempOutgoing, third, Seq(endNode -> target), header)

            val filterExpression = Not(Equals(startNode, endNode)(CTBoolean))(CTBoolean)
            val relsWithoutLoops = producer.planFilter(second, filterExpression, second.header)

            val tempIncoming = producer.planJoin(third, relsWithoutLoops, Seq(target -> startNode), third.header ++ second.header)
            val incoming = producer.planJoin(tempIncoming, first, Seq(endNode -> source), header)

            producer.planTabularUnionAll(outgoing, incoming)
        }

      case op@flat.ExpandInto(source, rel, target, direction, sourceOp, header, relHeader) =>
        val in = process(sourceOp)
        val relationships = producer.planRelationshipScan(in, op.sourceGraph, rel, relHeader)

        val startNode = StartNode(rel)()
        val endNode = EndNode(rel)()

        direction match {
          case Directed =>
            producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)

          case Undirected =>
            val outgoing = producer.planJoin(in, relationships, Seq(source -> startNode, target -> endNode), header)
            val incoming = producer.planJoin(in, relationships, Seq(target -> startNode, source -> endNode), header)
            producer.planTabularUnionAll(outgoing, incoming)
        }

      case flat.InitVarExpand(source, edgeList, endNode, in, header) =>
        producer.planInitVarExpand(process(in), source, edgeList, endNode, header)

      case flat.BoundedVarExpand(rel, edgeList, target, direction, lower, upper, sourceOp, relOp, targetOp, header, isExpandInto) =>
        val first = process(sourceOp)
        val second = process(relOp)
        val third = process(targetOp)

        producer.planBoundedVarExpand(
          first,
          second,
          third,
          rel,
          edgeList,
          target,
          sourceOp.endNode,
          lower,
          upper,
          direction,
          header,
          isExpandInto)

      case flat.Optional(lhs, rhs, header) => planOptional(lhs, rhs, header)

      case flat.ExistsSubQuery(predicateField, lhs, rhs, header) =>
        producer.planExistsSubQuery(process(lhs), process(rhs), predicateField, header)

      case flat.OrderBy(sortItems: Seq[SortItem[Expr]], in, header) =>
        producer.planOrderBy(process(in), sortItems, header)

      case flat.Skip(expr, in, header) => producer.planSkip(process(in), expr, header)

      case flat.Limit(expr, in, header) => producer.planLimit(process(in), expr, header)

      case flat.ReturnGraph(in) => producer.planReturnGraph(process(in))

      case other => throw NotImplementedException(s"Physical planning of operator $other")
    }
  }

  private def planConstructGraph(in: Option[FlatOperator], construct: LogicalPatternGraph)
    (implicit context: PhysicalPlannerContext[P, R]) = {
    val onGraphPlan = {
      construct.onGraphs match {
        case Nil => producer.planStart() // Empty start
        //TODO: Optimize case where no union is necessary
        //case h :: Nil => producer.planStart(Some(h)) // Just one graph, no union required
        case several =>
          val onGraphPlans = several.map(qgn => producer.planStart(Some(qgn)))
          producer.planGraphUnionAll(onGraphPlans, construct.name)
      }
    }
    val inputTablePlan = in.map(process).getOrElse(producer.planStart())

    val constructGraphPlan = producer.planConstructGraph(inputTablePlan, onGraphPlan, construct)
    context.constructedGraphPlans.update(construct.name, constructGraphPlan)
    constructGraphPlan
  }

  private def planOptional(lhs: FlatOperator, rhs: FlatOperator, header: RecordHeader)
    (implicit context: PhysicalPlannerContext[P, R]) = {
    val lhsData = process(lhs)
    val rhsData = process(rhs)
    val lhsHeader = lhs.header
    val rhsHeader = rhs.header

    def generateUniqueName = s"tmp${System.nanoTime}"

    // 1. Compute expressions between left and right side
    val commonExpressions = lhsHeader.expressions.intersect(rhsHeader.expressions)
    val joinExprs = commonExpressions.collect { case v: Var => v }
    val otherExpressions = commonExpressions -- joinExprs

    // 2. Remove siblings of the join expressions and other common fields
    val expressionsToRemove = joinExprs
      .flatMap(v => rhsHeader.ownedBy(v) - v)
      .union(otherExpressions)
    val rhsHeaderWithDropped = rhsHeader -- expressionsToRemove
    val rhsWithDropped = producer.planDrop(rhsData, expressionsToRemove, rhsHeaderWithDropped)

    // 3. Rename the join expressions on the right hand side, in order to make them distinguishable after the join
    val joinFieldRenames: Map[Expr, String] = joinExprs.map(e => e -> generateUniqueName).toMap
    val rhsHeaderWithRenames = rhsHeaderWithDropped.withColumnsRenamed(joinFieldRenames)
    val rhsWithRenamed = producer.planRenameColumns(rhsWithDropped, joinFieldRenames, rhsHeaderWithRenames)

    // 4. Left outer join the left side and the processed right side
    val joined = producer.planJoin(lhsData, rhsWithRenamed, joinExprs.map(e => e -> e).toSeq, rhsHeaderWithRenames ++ lhsHeader, LeftOuterJoin)

    // 5. Select the resulting header expressions
    producer.planSelect(joined, header.expressions.map(e => e -> Option.empty[Var]).toList, header)
  }
}
