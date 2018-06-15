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
package org.opencypher.spark.impl.physical

import org.opencypher.okapi.ir.api.util.DirectCompilationStage
import org.opencypher.okapi.trees.TopDown
import org.opencypher.spark.impl.physical.operators.{CAPSPhysicalOperator, Cache, DrivingTable}

case class PhysicalOptimizerContext()

class PhysicalOptimizer extends DirectCompilationStage[CAPSPhysicalOperator, CAPSPhysicalOperator, PhysicalOptimizerContext] {

  override def process(input: CAPSPhysicalOperator)(implicit context: PhysicalOptimizerContext): CAPSPhysicalOperator = {
    InsertCachingOperators(input)
  }

  object InsertCachingOperators extends (CAPSPhysicalOperator => CAPSPhysicalOperator) {
    def apply(input: CAPSPhysicalOperator): CAPSPhysicalOperator = {
      val replacements = calculateReplacementMap(input).filterKeys {
        case _: DrivingTable => false
        case _                           => true
      }

      val nodesToReplace = replacements.keySet

      TopDown[CAPSPhysicalOperator] {
        case cache: Cache => cache
        case parent if (parent.childrenAsSet intersect nodesToReplace).nonEmpty =>
          val newChildren = parent.children.map(c => replacements.getOrElse(c, c))
          parent.withNewChildren(newChildren)
      }.rewrite(input)
    }

    private def calculateReplacementMap(input: CAPSPhysicalOperator): Map[CAPSPhysicalOperator, CAPSPhysicalOperator] = {
      val opCounts = identifyDuplicates(input)
      val opsByHeight = opCounts.keys.toSeq.sortWith((a, b) => a.height > b.height)
      val (opsToCache, _) = opsByHeight.foldLeft(Set.empty[CAPSPhysicalOperator] -> opCounts) { (agg, currentOp) =>
        agg match {
          case (currentOpsToCache: Set[CAPSPhysicalOperator], currentCounts: Map[CAPSPhysicalOperator, Int]) =>
            val currentOpCount = currentCounts(currentOp)
            if (currentOpCount > 1) {
              val updatedOps = currentOpsToCache + currentOp
              val updatedCounts = currentCounts.map {
                case (op, count) => op -> (if (currentOp.containsTree(op)) count - 1 else count)
              }
              updatedOps -> updatedCounts
            } else {
              currentOpsToCache -> currentCounts
            }
        }
      }

      opsToCache.map(op => op -> Cache(op)).toMap
    }

    private def identifyDuplicates(input: CAPSPhysicalOperator): Map[CAPSPhysicalOperator, Int] = {
      input
        .foldLeft(Map.empty[CAPSPhysicalOperator, Int].withDefaultValue(0)) {
          case (agg, op) => agg.updated(op, agg(op) + 1)
        }
        .filter(_._2 > 1)
    }
  }
}
