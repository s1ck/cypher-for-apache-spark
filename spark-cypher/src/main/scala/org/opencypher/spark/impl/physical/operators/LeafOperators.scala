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
package org.opencypher.spark.impl.physical.operators

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, SchemaException}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.impl.CAPSRecords
import org.opencypher.spark.impl.physical.{CAPSPhysicalResult, CAPSRuntimeContext}

private[spark] abstract class LeafPhysicalOperator extends CAPSPhysicalOperator {

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    executeLeaf()
  }

  def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult
}

case object Empty extends LeafPhysicalOperator with PhysicalOperatorDebugging {

  override def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    CAPSPhysicalResult(
      CAPSRecords.empty(header)(context.session),
      resolve(context.session.emptyGraphQgn),
      context.session.emptyGraphQgn)

  override val header: RecordHeader = RecordHeader.empty
}

final case class EmptyWithHeader(header: RecordHeader)(implicit caps: CAPSSession)
  extends LeafPhysicalOperator with PhysicalOperatorDebugging {

  override def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    CAPSPhysicalResult(CAPSRecords.empty(header), resolve(context.session.emptyGraphQgn), context.session.emptyGraphQgn)
}

case object Unit extends LeafPhysicalOperator with PhysicalOperatorDebugging {
  override def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult =
    CAPSPhysicalResult(
      CAPSRecords.unit()(context.session),
      resolve(context.session.emptyGraphQgn),
      context.session.emptyGraphQgn)

  override def header: RecordHeader = RecordHeader.empty
}

final case class DrivingTable(records: CAPSRecords, header: RecordHeader)
  (implicit caps: CAPSSession) extends LeafPhysicalOperator with PhysicalOperatorDebugging {

  override def executeLeaf()(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    CAPSPhysicalResult(records, resolve(context.session.emptyGraphQgn), context.session.emptyGraphQgn)
  }

  override def toString: String = {
    s"DrivingTable(${records.logicalColumns.getOrElse(records.physicalColumns).mkString(", ")})"
  }

}

// TODO: move to special physical operator with variable number of input operators
final case class Scan(v: Var, header: RecordHeader, maybePatternGraph: Option[CAPSPhysicalOperator] = None)
  extends CAPSPhysicalOperator {

  override val children = maybePatternGraph.toArray

  override def withNewChildren(newChildren: Array[CAPSPhysicalOperator]): CAPSPhysicalOperator = {
    newChildren.toSeq match {
      case Seq() => copy(maybePatternGraph = None)
      case Seq(one) => copy(maybePatternGraph = Some(one))
      case _ => throw IllegalArgumentException("0 or 1 children", newChildren.mkString(", "))
    }
  }

  override def execute(implicit context: CAPSRuntimeContext): CAPSPhysicalResult = {
    // if there is a pattern graph it is created before performing the scan on it
    val maybeExecuted = maybePatternGraph.map(_.execute)

    val graphName = v.cypherType.graph.get

    val workingGraph = maybeExecuted match {
      case Some(physicalResult) => physicalResult.workingGraph
      case None => resolve(graphName)
    }

    val records = v.cypherType match {
      case n: CTNode => workingGraph.nodes(v.name, n)
      case r: CTRelationship => workingGraph.relationships(v.name, r)
      case other => throw IllegalArgumentException("Node variable", other)
    }
    if (header != records.header) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed record header based on graph schema: ${header.pretty}
           |  - Actual record header: ${records.header.pretty}
        """.stripMargin)
    }
    CAPSPhysicalResult(records, workingGraph, graphName)
  }
}

