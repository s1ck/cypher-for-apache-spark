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

import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr.{Aggregator, Explode, Expr, Var}
import org.opencypher.okapi.logical.impl.{Direction, LogicalGraph}
import org.opencypher.okapi.relational.impl.table._
import org.opencypher.okapi.trees.AbstractTreeNode

sealed abstract class FlatOperator extends AbstractTreeNode[FlatOperator] {
  def header: RecordHeader

  def sourceGraph: LogicalGraph

  def withHeader(newHeader: RecordHeader): FlatOperator
}

sealed abstract class BinaryFlatOperator extends FlatOperator {
  def lhs: FlatOperator
  def rhs: FlatOperator

  override def sourceGraph: LogicalGraph = lhs.sourceGraph
}

sealed abstract class TernaryFlatOperator extends FlatOperator {
  def first: FlatOperator
  def second: FlatOperator
  def third: FlatOperator

  override def sourceGraph: LogicalGraph = first.sourceGraph
}

sealed abstract class StackingFlatOperator extends FlatOperator {
  def in: FlatOperator

  override def sourceGraph: LogicalGraph = in.sourceGraph
}

sealed abstract class FlatLeafOperator extends FlatOperator

final case class NodeScan(nodeType: CTNode, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class RelationshipScan(
  relType: CTRelationship,
  in: FlatOperator,
  header: RecordHeader
) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Filter(expr: Expr, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Distinct(fields: Set[Var], in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Select(fields: List[Var], in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class ReturnGraph(in: FlatOperator) extends StackingFlatOperator {
  override def header: RecordHeader = RecordHeader.empty

  override def withHeader(newHeader: RecordHeader): FlatOperator =
    if (newHeader != header) throw UnsupportedOperationException("ReturnGraph only supports empty header") else this
}

final case class Project(
  expr: Expr,
  alias: Option[Var],
  in: FlatOperator,
  header: RecordHeader
) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Unwind(expr: Explode, item: Var, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Aggregate(
  aggregations: Set[(Var, Aggregator)],
  group: Set[Var],
  in: FlatOperator,
  header: RecordHeader
)
  extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Alias(expr: Expr, alias: Var, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class CartesianProduct(
  lhs: FlatOperator,
  rhs: FlatOperator,
  header: RecordHeader
) extends BinaryFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Optional(lhs: FlatOperator, rhs: FlatOperator, header: RecordHeader) extends BinaryFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class ExistsSubQuery(predicateField: Var, lhs: FlatOperator, rhs: FlatOperator, header: RecordHeader)
  extends BinaryFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class ValueJoin(
  lhs: FlatOperator,
  rhs: FlatOperator,
  predicates: Set[org.opencypher.okapi.ir.api.expr.Equals],
  header: RecordHeader
)
  extends BinaryFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Expand(
  source: Var,
  rel: Var,
  direction: Direction,
  target: Var,
  sourceOp: FlatOperator,
  targetOp: FlatOperator,
  header: RecordHeader,
  relHeader: RecordHeader
)
  extends BinaryFlatOperator {

  override def lhs: FlatOperator = sourceOp
  override def rhs: FlatOperator = targetOp

  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class ExpandInto(
  source: Var,
  rel: Var,
  target: Var,
  direction: Direction,
  sourceOp: FlatOperator,
  header: RecordHeader,
  relHeader: RecordHeader
) extends StackingFlatOperator {

  override def in: FlatOperator = sourceOp

  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class InitVarExpand(source: Var, edgeList: Var, endNode: Var, in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class BoundedVarExpand(
  rel: Var,
  edgeList: Var,
  target: Var,
  direction: Direction,
  lower: Int,
  upper: Int,
  sourceOp: InitVarExpand,
  relOp: FlatOperator,
  targetOp: FlatOperator,
  header: RecordHeader,
  isExpandInto: Boolean
) extends TernaryFlatOperator {

  override def first: FlatOperator = sourceOp
  override def second: FlatOperator = relOp
  override def third: FlatOperator = targetOp

  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class OrderBy(sortItems: Seq[SortItem[Expr]], in: FlatOperator, header: RecordHeader)
  extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Skip(expr: Expr, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Limit(expr: Expr, in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class EmptyRecords(in: FlatOperator, header: RecordHeader) extends StackingFlatOperator {
  override def withHeader(newHeader: RecordHeader): FlatOperator = copy(header = newHeader)
}

final case class Start(sourceGraph: LogicalGraph, vars: Set[Var]) extends FlatLeafOperator {
  override val header: RecordHeader = RecordHeader.from(vars)

  override def withHeader(newHeader: RecordHeader): FlatOperator =
    if (newHeader != header) throw UnsupportedOperationException("Start cannot update header") else this
}

final case class FromGraph(override val sourceGraph: LogicalGraph, in: FlatOperator)
  extends StackingFlatOperator {
  override def header: RecordHeader = in.header

  override def withHeader(newHeader: RecordHeader): FlatOperator =
    if (newHeader != header) throw UnsupportedOperationException("FromGraph cannot update header") else this
}
