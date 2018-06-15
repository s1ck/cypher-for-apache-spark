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
package org.opencypher.okapi.logical.impl

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, IllegalStateException, NotImplementedException}
import org.opencypher.okapi.ir.api.block._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.pattern._
import org.opencypher.okapi.ir.api.set.SetPropertyItem
import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.ir.api.{Label, _}
import org.opencypher.okapi.ir.impl.syntax.ExprSyntax._
import org.opencypher.okapi.ir.impl.util.VarConverters._
import org.opencypher.okapi.logical.impl.exception.{InvalidCypherTypeException, InvalidDependencyException, InvalidPatternException}

import scala.annotation.tailrec

class LogicalPlanner(producer: LogicalOperatorProducer)
  extends DirectCompilationStage[CypherQuery[Expr], LogicalOperator, LogicalPlannerContext] {

  override def process(ir: CypherQuery[Expr])(implicit context: LogicalPlannerContext): LogicalOperator = {
    val model = ir.model

    planModel(model.result, model)
  }

  def planModel(block: ResultBlock[Expr], model: QueryModel[Expr])(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    val first = block.after.head // there should only be one, right?

    val plan = planBlock(first, model, None)

    // always plan a select at the top
    block match {
      case t: TableResultBlock[_] =>
        val fields = t.binds.orderedFields.map(f => Var(f.name)(f.cypherType))
        producer.planSelect(fields, plan)
      case g: GraphResultBlock[_] =>
        producer.planReturnGraph(producer.planFromGraph(resolveGraph(g.graph, plan.fields), plan))
    }
  }

  final def planBlock(block: Block[Expr], model: QueryModel[Expr], plan: Option[LogicalOperator])(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    if (block.after.isEmpty) {
      // this is a leaf block, just plan it
      planLeaf(block, model)
    } else if (plan.nonEmpty && plan.get.solved.contains(block.after.toSet)) {
      // all deps satisfied for this block, we can just plan it if we have already planned a leaf
      planNonLeaf(block, model, plan.get)
    } else {
      // either we haven't planned a leaf yet, or the block is not ready to be planned
      // plan one of the block dependencies
      val depRef = plan match {
        case None =>
          // nothing has been planned, just pick one
          block.after.head
        case Some(_plan) =>
          // we need to plan a block that hasn't already been solved
          // TODO: refactor to remove illegal state
          block.after
            .find(r => !_plan.solved.contains(r))
            .getOrElse(throw IllegalStateException("Found block with unsolved dependencies which cannot be solved."))
      }
      val dependency = planBlock(depRef, model, plan)
      planBlock(block, model, Some(dependency))
    }
  }

  def planLeaf(block: Block[Expr], model: QueryModel[Expr])
    (implicit context: LogicalPlannerContext): LogicalOperator = {
    block match {
      case SourceBlock(_) => producer.planDrivingTable(context.inputRecordFields)
      case x => throw NotImplementedException(s"Support for leaf planning of $x not yet implemented. Tree:\n${x.pretty}")
    }
  }

  def planNonLeaf(block: Block[Expr], model: QueryModel[Expr], plan: LogicalOperator)(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    block match {
      case MatchBlock(_, pattern, where, optional, graph) =>
        val logicalGraph = resolveGraph(graph, plan.fields)
        val inputGraphPlan = if (plan.graph == logicalGraph) {
          plan
        } else {
          planFromGraph(logicalGraph, plan)
        }
        // this plans both pattern and filter for convenience -- TODO: split up
        val patternPlan = planMatchPattern(inputGraphPlan, pattern, where, graph)
        if (optional) producer.planOptional(inputGraphPlan, patternPlan) else patternPlan

      case ProjectBlock(_, Fields(fields), where, _, distinct) =>
        val withFields = planFieldProjections(plan, fields)
        val filtered = planFilter(withFields, where)
        if (distinct) {
          producer.planDistinct(fields.keySet, filtered)
        } else {
          filtered
        }

      case OrderAndSliceBlock(_, sortItems, skip, limit, _) =>
        val orderOp = if (sortItems.nonEmpty) producer.planOrderBy(sortItems, plan) else plan

        val skipOp = skip match {
          case Some(expr) => producer.planSkip(expr, orderOp)
          case None => orderOp
        }

        limit match {
          case Some(expr) => producer.planLimit(expr, skipOp)
          case None => skipOp
        }

      case AggregationBlock(_, a@Aggregations(pairs), group, _) =>
        // plan projection of aggregation argument
        val prev = pairs.foldLeft(plan) {
          case (prevPlan, (_, agg)) =>
            agg match {
              case a: Aggregator => a.inner.map(e => planInnerExpr(e, prevPlan)).getOrElse(prevPlan)
              case _ => throw IllegalArgumentException("an aggregator", agg)
            }
        }
        producer.aggregate(a, group, prev)

      case UnwindBlock(_, UnwoundList(list, variable), _) =>
        val withList = planInnerExpr(list, plan)
        producer.planUnwind(list, variable, withList)

      case GraphResultBlock(_, graph) =>
        producer.planReturnGraph(producer.planFromGraph(resolveGraph(graph, plan.fields), plan))

      case x =>
        throw NotImplementedException(s"Support for logical planning of $x not yet implemented. Tree:\n${x.pretty}")
    }
  }

  private def planFieldProjections(in: LogicalOperator, exprs: Map[IRField, Expr])(
    implicit context: LogicalPlannerContext
  ) = {
    exprs.foldLeft(in) {
      case (acc, (f, p: Property)) =>
        producer.projectField(p, f, acc)

      case (acc, (f, func: FunctionExpr)) =>
        val projectArg = func.exprs.foldLeft(acc) {
          case (acc2, expr) => planInnerExpr(expr, acc2)
        }
        producer.projectField(func, f, projectArg)

      // this is for aliasing
      case (acc, (f, v: Var)) if f.name != v.name =>
        producer.projectField(v, f, acc)

      case (acc, (_, _: Var)) =>
        acc

      case (acc, (f, be: BinaryExpr)) =>
        val projectLhs = planInnerExpr(be.lhs, acc)
        val projectRhs = planInnerExpr(be.rhs, projectLhs)
        producer.projectField(be, f, projectRhs)

      case (acc, (f, c: Param)) =>
        producer.projectField(c, f, acc)

      case (acc, (f, c: Lit[_])) =>
        producer.projectField(c, f, acc)

      case (acc, (f, ex: ExistsPatternExpr)) =>
        val subqueryPlan = this (ex.ir)
        val existsPlan = producer.planExistsSubQuery(ex, acc, subqueryPlan)
        producer.projectField(existsPlan.expr.targetField, f, existsPlan)

      case (acc, (f, e: PredicateExpression)) =>
        val projectInner = planInnerExpr(e.inner, acc)
        producer.projectField(e, f, projectInner)

      case (acc, (f, c: CaseExpr)) =>
        val (leftExprs, rightExprs) = c.alternatives.unzip
        val plannedAlternatives = (leftExprs ++ rightExprs).foldLeft(acc)((op, expr) => planInnerExpr(expr, op))

        val plannedAll = c.default match {
          case Some(inner) => planInnerExpr(inner, plannedAlternatives)
          case None => plannedAlternatives
        }
        producer.projectField(c, f, plannedAll)

      case (acc, (f, a@Ands(inner))) =>
        val plannedInner = inner.foldLeft(acc)((op, expr) => planInnerExpr(expr, op))
        producer.projectField(a, f, plannedInner)

      case (_, (_, x)) =>
        throw NotImplementedException(s"Support for projection of $x not yet implemented. Tree:\n${x.pretty}")
    }
  }

  // TODO: Should we check (or silently drop) predicates that are not eligible for planning here? (check dependencies)
  private def planFilter(in: LogicalOperator, where: Set[Expr])(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    val filtersAndProjs = where.foldLeft(in) {
      case (acc, ors: Ors) =>
        val withInnerExprs = ors.exprs.foldLeft(acc) {
          case (_acc, expr) => planInnerExpr(expr, _acc)
        }
        producer.planFilter(ors, withInnerExprs)

      case (acc, eq: Equals) =>
        val project1 = planInnerExpr(eq.lhs, acc)
        val project2 = planInnerExpr(eq.rhs, project1)
        producer.planFilter(eq, project2)

      case (acc, be: BinaryExpr) =>
        val project1 = planInnerExpr(be.lhs, acc)
        val project2 = planInnerExpr(be.rhs, project1)
        val projectParent = producer.projectExpr(be, project2)
        producer.planFilter(be, projectParent)

      case (acc, h@HasLabel(_: Var, _)) =>
        producer.planFilter(h, acc)

      case (acc, not@Not(Equals(lhs, rhs))) =>
        val p1 = planInnerExpr(lhs, acc)
        val p2 = planInnerExpr(rhs, p1)
        producer.planFilter(not, p2)

      case (acc, not@Not(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(not, project)

      case (acc, exists@Exists(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(exists, project)

      case (acc, isNull@IsNull(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(isNull, project)

      case (acc, isNotNull@IsNotNull(expr)) =>
        val project = planInnerExpr(expr, acc)
        producer.planFilter(isNotNull, project)

      case (acc, t: TrueLit) =>
        producer.planFilter(t, acc) // optimise away this one somehow... currently we do that in PhysicalPlanner

      case (acc, v: Var) =>
        producer.planFilter(v, acc)

      case (acc, ex: ExistsPatternExpr) =>
        val innerPlan = this (ex.ir)
        val predicate = producer.planExistsSubQuery(ex, acc, innerPlan)
        producer.planFilter(ex, predicate)

      case (_, x) =>
        throw NotImplementedException(s"Support for logical planning of predicate $x not yet implemented. Tree:\n${x.pretty}")
    }

    filtersAndProjs
  }

  private def planInnerExpr(expr: Expr, in: LogicalOperator)(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {
    expr match {
      case _: Param => in

      case ListLit(exprs) =>
        exprs.foldLeft(in) { case (acc, inner) => planInnerExpr(inner, acc) }

      case _: Lit[_] => in

      case _: Var => in

      case p: Property =>
        producer.projectExpr(p, in)

      case be: BinaryExpr =>
        val project1 = planInnerExpr(be.lhs, in)
        val project2 = planInnerExpr(be.rhs, project1)
        producer.projectExpr(be, project2)

      case HasLabel(e, _) => planInnerExpr(e, in)

      case Not(e) => planInnerExpr(e, in)

      case IsNull(e) => planInnerExpr(e, in)

      case IsNotNull(e) => planInnerExpr(e, in)

      case func: FunctionExpr =>
        val projectArg = func.exprs.foldLeft(in) {
          case (acc, e) => planInnerExpr(e, acc)
        }
        producer.projectExpr(func, projectArg)

      case ex: ExistsPatternExpr =>
        val innerPlan = this (ex.ir)
        producer.planExistsSubQuery(ex, in, innerPlan)

      case ands@Ands(inner) =>
        val plannedInner = inner.foldLeft(in)((op, expr) => planInnerExpr(expr, op))
        producer.projectExpr(ands, plannedInner)

      case x =>
        throw NotImplementedException(s"Support for projection of inner expression $x not yet implemented. Tree:\n${x.pretty}")
    }
  }

  private def resolveGraph(graph: IRGraph, fieldsInScope: Set[Var] = Set.empty)(
    implicit context: LogicalPlannerContext
  ): LogicalGraph = {

    graph match {
      // TODO: IRGraph[Expr]
      case p: IRPatternGraph[Expr@unchecked] =>
        import org.opencypher.okapi.ir.impl.util.VarConverters.RichIrField
        val baseEntities = p.news.baseFields.mapValues(_.toVar)

        val clonePatternEntities = p.clones.keys

        val newPatternEntities = p.news.fields

        val entitiesToCreate = newPatternEntities -- clonePatternEntities

        val clonedVarToInputVar: Map[Var, Var] = p.clones.map { case (clonedField, inputExpression) =>
          val inputVar = inputExpression match {
            case v: Var => v
            case other => throw IllegalArgumentException("CLONED expression to be a variable", other)
          }
          clonedField.toVar -> inputVar
        }

        val newEntities: Set[ConstructedEntity] = entitiesToCreate.map(e => extractConstructedEntities(p.news, e, baseEntities.get(e)))

        val setItems = {
          p.news.properties.flatMap { case (irField, mapExpr) =>
            val v = irField.toVar
            mapExpr.items.map { case (propertyKey, expr) =>
              SetPropertyItem(propertyKey, v, expr)
            }
          }
        }.toList

        LogicalPatternGraph(p.schema, clonedVarToInputVar, newEntities, setItems, p.onGraphs, p.qualifiedGraphName)

      case g: IRCatalogGraph => LogicalCatalogGraph(g.qualifiedGraphName, g.schema)
    }
  }

  private def extractConstructedEntities(
    pattern: Pattern[Expr],
    e: IRField,
    baseField: Option[Var]
  ) = e.cypherType match {
    case CTRelationship(relTypes, _) if relTypes.size <= 1 =>
      val connection = pattern.topology(e)
      ConstructedRelationship(e, connection.source, connection.target, relTypes.headOption, baseField)
    case CTNode(labels, _) =>
      ConstructedNode(e, labels.map(Label), baseField)
    case other =>
      throw InvalidCypherTypeException(s"Expected an entity type (CTNode, CTRelationship), got $other")
  }

  private def planFromGraph(graph: LogicalGraph, prev: LogicalOperator)(
    implicit context: LogicalPlannerContext
  ): FromGraph = {

    producer.planFromGraph(graph, prev)
  }

  private def planMatchPattern(plan: LogicalOperator, pattern: Pattern[Expr], where: Set[Expr], graph: IRGraph)(
    implicit context: LogicalPlannerContext
  ) = {
    val components = pattern.components.toSeq
    if (components.size == 1) {
      val patternPlan = planComponentPattern(plan, components.head, graph)
      val filteredPlan = planFilter(patternPlan, where)
      filteredPlan
    } else {
      // TODO: Find a way to feed the same input into all arms of the cartesian product without recomputing it
      val bases = plan +: components.map(_ => DrivingTable(SolvedQueryModel.empty)).tail
      val plans = bases.zip(components).map {
        case (base, component) =>
          val componentPlan = planComponentPattern(base, component, graph)
          val predicates = where.filter(_.canEvaluate(componentPlan.fields)).filterNot(componentPlan.solved.predicates)
          val filteredPlan = planFilter(componentPlan, predicates)
          filteredPlan
      }
      val result = plans.reduceOption { (lhs, rhs) =>
        val fieldsInScope = lhs.fields ++ rhs.fields
        val solvedPredicates = lhs.solved.predicates ++ rhs.solved.predicates
        val predicates = where.filter(_.canEvaluate(fieldsInScope)).filterNot(solvedPredicates)
        val joinPredicates = predicates.collect { case e: Equals => e }
        if (joinPredicates.isEmpty) {
          val combinedPlan = producer.planCartesianProduct(lhs, rhs)
          val filteredPlan = planFilter(combinedPlan, predicates)
          filteredPlan
        } else {
          val (leftIn, rightIn) = joinPredicates.foldLeft((lhs, rhs)) {
            case ((l, r), predicate) => producer.projectExpr(predicate.lhs, l) -> producer.projectExpr(predicate.rhs, r)
          }
          producer.planValueJoin(leftIn, rightIn, joinPredicates)
        }
      }
      // TODO: use type system to avoid empty pattern
      result.getOrElse(throw InvalidPatternException("Cannot plan an empty match pattern"))
    }
  }

  private def planComponentPattern(plan: LogicalOperator, pattern: Pattern[Expr], graph: IRGraph)(
    implicit context: LogicalPlannerContext
  ): LogicalOperator = {

    val nodes = pattern.fields.filter(_.cypherType.subTypeOf(CTNode).isTrue)
    val node = nodes.head
    val resolvedGraph = resolveGraph(graph)
    val nodePlan = producer.planNodeScan(node, resolvedGraph)
    // find all unsolved nodes from the pattern

    if (pattern.topology.isEmpty) { // there is no connection in the pattern => plan a node scan
      // if we have already have a previous result we need to plan a cartesian product
      if (plan.fields.nonEmpty) {
        producer.planCartesianProduct(plan, nodePlan)
      } else { // first node scan
        nodePlan
      }
    } else { // there are connections => we need to expand

      val solved = nodes.intersect(plan.solved.fields)
      val unsolved = nodes -- solved

      val (firstPlan, remaining) = if (solved.isEmpty) { // there is no connection to the previous plan
        if (plan.solved.fields.nonEmpty) { // there are already fields in the previous plan, we need to plan a cartesian product
          producer.planCartesianProduct(plan, nodePlan) -> nodes.tail
        } else { // there are no previous results, it's safe to plan a node scan
          nodePlan -> nodes.tail
        }
      } else { // we can connect to the previous plan
        plan -> unsolved
      }

      val remainingPlans: Set[LogicalOperator] = remaining
        .map { irField =>
          val maybePatternGraph = resolvedGraph match {
            case p: LogicalPatternGraph =>
              Some(producer.planFromGraph(p, Empty(Set.empty, LogicalEmptyGraph, SolvedQueryModel.empty)))
            case _ => None
          }
          producer.planNodeScan(irField, resolvedGraph, maybePatternGraph)
        }.toSet

      // tie all plans together using expansions
      planExpansions(remainingPlans + firstPlan, pattern, producer)
    }
  }

  @tailrec
  private def planExpansions(
    disconnectedPlans: Set[LogicalOperator],
    pattern: Pattern[Expr],
    producer: LogicalOperatorProducer
  ): LogicalOperator = {
    val allSolved = disconnectedPlans.map(_.solved).reduce(_ ++ _)

    val (r, c) = pattern.topology.collectFirst {
      case (rel, conn: Connection) if !allSolved.solves(rel) =>
        rel -> conn
    }.getOrElse(
      // TODO: exclude case with type system
      throw InvalidPatternException("Cannot plan an expansion that has already been solved")
    )

    val sourcePlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.source) => p
    }.getOrElse(throw InvalidDependencyException("Cannot plan expansion for unsolved source plan"))
    val targetPlan = disconnectedPlans.collectFirst {
      case p if p.solved.solves(c.target) => p
    }.getOrElse(throw InvalidDependencyException("Cannot plan expansion for unsolved target plan"))

    val expand = c match {
      case v: VarLengthRelationship if v.upper.nonEmpty =>
        val direction = v match {
          case _: DirectedVarLengthRelationship => Directed
          case _: UndirectedVarLengthRelationship => Undirected
        }
        producer.planBoundedVarLengthExpand(c.source, r, c.target, direction, v.lower, v.upper.get, sourcePlan, targetPlan)

      case _: UndirectedConnection if sourcePlan == targetPlan =>
        producer.planExpandInto(c.source, r, c.target, Undirected, sourcePlan)
      // cyclic and directed are handled the same way for expandInto

      case _ if sourcePlan == targetPlan =>
        producer.planExpandInto(c.source, r, c.target, Directed, sourcePlan)

      case _: DirectedConnection =>
        producer.planExpand(c.source, r, c.target, Directed, sourcePlan, targetPlan)

      case _: UndirectedConnection =>
        producer.planExpand(c.source, r, c.target, Undirected, sourcePlan, targetPlan)

    }

    if (expand.solved.solves(pattern)) expand
    else planExpansions((disconnectedPlans - sourcePlan - targetPlan) + expand, pattern, producer)
  }
}
