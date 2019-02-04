/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.okapi.relational.impl.planning

import org.opencypher.okapi.relational.api.table.Table
import org.opencypher.okapi.relational.impl.operators._
import org.opencypher.okapi.trees.{BottomUpWithContext, TopDown}
import RelationalPlanner._
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}

import scala.reflect.runtime.universe.TypeTag

object RelationalOptimizer {

  def process[T <: Table[T] : TypeTag](input: RelationalOperator[T]): RelationalOperator[T] = {
    InsertCachingOperators(JoinReordering(input))
  }

  object JoinReordering {

    private val defaultCardinality = 1

    def apply[T <: Table[T] : TypeTag](input: RelationalOperator[T]): RelationalOperator[T] = {

      val (output, context) = BottomUpWithContext[RelationalOperator[T], Map[RelationalOperator[T], Long]] {
        case (join @ Join(lhs, rhs, joinExprs, InnerJoin), context) => {
          join -> context.updated(join, context(lhs) * context(rhs))
        }
        case (union : TabularUnionAll[T], context) =>
          union -> context.updated(union, context(union.lhs) + context(union.rhs))

        case (start @ Start(gqn, Some(_), _), context) =>
          start.graph.maybeStatistics match {
            case Some(statistics) =>
              val entity = start.singleEntity
              val cardinality: Long = entity.cypherType match {

                case CTNode(labels, _) =>
                  statistics.nodeCounts.getOrElse(labels, defaultCardinality)

                case CTRelationship(relTypes, _) if relTypes.size == defaultCardinality =>
                  statistics.relCounts.getOrElse(relTypes.head, defaultCardinality)
              }
              start -> context.updated(start, cardinality)

            case None =>
              start -> context.updated(start, defaultCardinality)
          }

        case (other, context) =>
          other -> context.updated(other, context(other.children.head))

      }.transform(input, Map.empty)

      context.foreach { case (op, cardinality) => println(s"[$cardinality] $op") }

      output
    }
  }

  object InsertCachingOperators {

    def apply[T <: Table[T] : TypeTag](input: RelationalOperator[T]): RelationalOperator[T] = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: Start[T] => false
        case _ => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[RelationalOperator[T]] {
        case cache: Cache[T] => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }.transform(input)
    }

    private def calculateReplacementMap[T <: Table[T] : TypeTag](input: RelationalOperator[T]): Map[RelationalOperator[T], RelationalOperator[T]] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[RelationalOperator[T]] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache, currentCounts) =>
            val currentOpCount = currentCounts(currentOp)
            if (currentOpCount > 1) {
              val updatedOps = currentOpsToCache + currentOp
              // We're traversing `opsByHeight` from largest to smallest query sub-tree.
              // We pick the trees with the largest height for caching first, and then reduce the duplicate count
              // for the sub-trees of the cached tree by the number of times the parent tree appears.
              // The idea behind this is that if the parent was already cached, there is no need to additionally
              // cache all its children (unless they're used with a different parent somewhere else).
              val updatedCounts = currentCounts.map {
                case (op, count) => op -> (if (currentOp.containsTree(op)) count - currentOpCount else count)
              }
              updatedOps -> updatedCounts
            } else {
              currentOpsToCache -> currentCounts
            }
        }
      }
      opsToCache.map(op => op -> Cache[T](op)).toMap
    }

    private def identifyDuplicates[T <: Table[T]](input: RelationalOperator[T]): Map[RelationalOperator[T], Int] = {
      input
        .foldLeft(Map.empty[RelationalOperator[T], Int].withDefaultValue(0)) {
          case (agg, op) => agg.updated(op, agg(op) + 1)
        }
        .filter(_._2 > 1)
    }
  }
}
