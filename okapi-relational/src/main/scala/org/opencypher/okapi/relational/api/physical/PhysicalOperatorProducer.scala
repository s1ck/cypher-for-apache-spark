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
package org.opencypher.okapi.relational.api.physical

import org.opencypher.okapi.api.graph.{PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.ir.api.block.SortItem
import org.opencypher.okapi.ir.api.expr.{Aggregator, Expr, Var}
import org.opencypher.okapi.logical.impl._
import org.opencypher.okapi.relational.api.io.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.physical.{InnerJoin, JoinType}
import org.opencypher.okapi.relational.impl.table.RecordHeader

/**
  * Main interface to be implemented by custom (relational) back-ends to execute a Cypher query. Methods are being
  * called by [[org.opencypher.okapi.relational.impl.physical.PhysicalPlanner]], a default implementation for physical planning.
  *
  * @tparam O backend-specific flat relational table
  * @tparam K backend-specific physical operators
  * @tparam A backend-specific cypher records
  * @tparam P backend-specific property graph
  * @tparam I backend-specific runtime context
  */
trait PhysicalOperatorProducer[
O <: FlatRelationalTable[O],
K <: PhysicalOperator[A, P, I],
A <: RelationalCypherRecords[O],
P <: PropertyGraph,
I <: RuntimeContext[A, P]] {

  // Leaf operators

  /**
    * Leaf operator that starts from empty records.
    *
    * @return empty graph
    */
  def planEmpty: K

  /**
    * Leaf operator that starts from empty records with predefined header.
    *
    * @return empty graph
    */
  def planEmptyWithHeader(header: RecordHeader): K

  /**
    * Leaf operator that starts from unit records.
    *
    * @return empty graph
    */
  def planUnit: K

  /**
    * Starts the query execution based on optional given records and an optional graph.
    *
    * @param drivingRecords     backend-specific records
    * @return driving table operator
    */
  def planDrivingTable(drivingRecords: A, header: RecordHeader = RecordHeader.empty): K

  /**
    * Scans the node set of the input graph and returns all nodes that match the given CTNode type.
    *
    * @param v       node variable carrying the node type to scan for
    * @param header  resulting record header
    * @return node scan operator
    */
  def planNodeScan(v: Var, header: RecordHeader, maybePatternGraph: Option[K] = None): K

  /**
    * Scans the relationship set of the input graph and returns all relationships that match the given CTRelationship
    * type.
    *
    * @param v       node variable carrying the relationship type to scan for
    * @param header  resulting record header
    * @return relationship scan operator
    */
  def planRelationshipScan(v: Var, header: RecordHeader, maybePatternGraph: Option[K] = None): K

  // Unary operators

  /**
    * Renames the columns identified by the given expressions to the specified aliases.
    *
    * @param in     previous operator
    * @param tuples pairs of source expressions to target aliases
    * @param header resulting record header
    * @return Alias operator
    */
  def planAlias(in: K, tuples: Seq[(Expr, Var)], header: RecordHeader): K

  /**
    * Renames the column identified by the given expression to the specified alias.
    *
    * @param in     previous operator
    * @param expr   expression to be aliased
    * @param alias  alias
    * @param header resulting record header
    * @return Alias operator
    */
  def planAlias(in: K, expr: Expr, alias: Var, header: RecordHeader): K = planAlias(in, Seq(expr -> alias), header)

  /**
    * Drops the columns identified by the given expressions from the input records.
    * Expressions not present are ignored.
    *
    * @param in         previous operator
    * @param dropFields expressions to be dropped
    * @param header     resulting record header
    * @return Drop operator
    */
  def planDrop(in: K, dropFields: Set[Expr], header: RecordHeader): K

  /**
    * Renames the columns associated with the given expressions to the specified new column names.
    *
    * @param in          previous operator
    * @param renameExprs expressions to new columns
    * @param header      resulting record header
    * @return Rename operator
    */
  def planRenameColumns(in: K, renameExprs: Map[Expr, String], header: RecordHeader): K

  /**
    * Filters the incoming rows according to the specified expression.
    *
    * @param in     previous operator
    * @param expr   expression to be evaluated
    * @param header resulting record header
    * @return filter operator
    */
  def planFilter(in: K, expr: Expr, header: RecordHeader): K

  /**
    * Selects the specified expressions from the given records.
    *
    * @param in          previous operator
    * @param expressions expressions to select from the records
    * @param header      resulting record header
    * @return select operator
    */
  def planSelect(in: K, expressions: List[(Expr, Option[Var])], header: RecordHeader): K

  /**
    * Returns the working graph
    *
    * @param in previous operator
    */
  def planReturnGraph(in: K): K

  /**
    * Use the specified graph.
    *
    * @param maybeIn previous operator
    * @param qgn graph to select from the catalog
    * @return select graph operator
    */
  def planFromGraph(maybeIn: Option[K], qgn: QualifiedGraphName): K

  /**
    * Evaluates the given expression and projects it to a new column in the input records.
    *
    * @param in     previous operator
    * @param expr   expression to evaluate
    * @param alias  alias to project expr to
    * @param header resulting record header
    * @return project operator
    */
  def planProject(in: K, expr: Expr, alias: Option[Var], header: RecordHeader): K

  /**
    * Creates a new record containing the specified entities (i.e. as defined in a construction pattern).
    *
    * @param table     table that contains cloned aliases and data for constructing new entities
    * @param construct graph to construct
    * @return project pattern graph operator
    */
  def planConstructGraph(table: K, construct: LogicalPatternGraph): K

  /**
    * Creates a new record containing the specified entities (i.e. as defined in a construction pattern).
    *
    * @param table     table that contains cloned aliases and data for constructing new entities
    * @param onGraph   graph that we construct on
    * @param construct graph to construct
    * @return project pattern graph operator
    */
  def planConstructOnGraph(table: K, onGraph: K, construct: LogicalPatternGraph): K

  /**
    * Groups the underlying records by the specified expressions and evaluates the given aggregate functions.
    *
    * @param in           previous operator
    * @param group        vars to group records by
    * @param aggregations aggregate functions
    * @param header       resulting record header
    * @return aggregate operator
    */
  def planAggregate(in: K, group: Set[Var], aggregations: Set[(Var, Aggregator)], header: RecordHeader): K

  /**
    * Performs a distinct operation on the specified fields.
    *
    * @param in     previous operator
    * @param fields fields to compute distinct on
    * @return distinct operator
    */
  def planDistinct(in: K, fields: Set[Var]): K

  /**
    * Orders the underlying records by the given expressions.
    *
    * @param in        previous operator
    * @param sortItems fields to order records by
    * @param header    resulting record header
    * @return order by operator
    */
  def planOrderBy(in: K, sortItems: Seq[SortItem[Expr]], header: RecordHeader): K

  /**
    * Initializes the underlying records for a variable expand computation (e.g., (a)-[:A*1..3]->(b)).
    *
    * @param in       previous operator
    * @param source   variable to expand from (e.g. (a))
    * @param edgeList variable to identify column which later stores relationship identifiers of the computed paths
    * @param target   variable to expand into (e.g. (b))
    * @param header   resulting record header
    * @return init var expand operator
    */
  def planInitVarExpand(in: K, source: Var, edgeList: Var, target: Var, header: RecordHeader): K

  /**
    * Skips the given amount of rows in the input records. The number of rows is specified by an expression which can be
    * a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the number of rows to skip
    * @param header resulting record header
    * @return skip operator
    */
  def planSkip(in: K, expr: Expr, header: RecordHeader): K

  /**
    * Limits the number of input records to the specified amount. The number of rows is specified by an expression which
    * can be a literal or a query parameter.
    *
    * @param in     previous operator
    * @param expr   expression which contains or refers to the maximum number of rows to return
    * @param header resulting record header
    * @return limit operator
    */
  def planLimit(in: K, expr: Expr, header: RecordHeader): K

  // Binary operators

  /**
    * Computes a cartesian product between the two input records.
    *
    * @param lhs    first previous operator
    * @param rhs    second previous operator
    * @param header resulting record header
    * @return cross operator
    */
  def planCartesianProduct(lhs: K, rhs: K, header: RecordHeader): K

  /**
    * Joins the two input records using the given expressions.
    *
    * @param lhs         first previous operator
    * @param rhs         second previous operator
    * @param joinColumns sequence of left and right join columns
    * @param header      resulting record header
    * @param joinType    type of the join
    * @return join operator
    */
  def planJoin(lhs: K, rhs: K, joinColumns: Seq[(Expr, Expr)], header: RecordHeader, joinType: JoinType = InnerJoin): K

  /**
    * Unions the input records.
    *
    * @param lhs first previous operator
    * @param rhs second previous operator
    * @return union operator
    */
  def planTabularUnionAll(lhs: K, rhs: K): K

  /**
    * Filters the rows of the first input by checking if there exists a corresponding row in the second input.
    *
    * @param lhs         first previous operator
    * @param rhs         second previous operator
    * @param targetField field that stores the (boolean) result of the evaluation
    * @param header      resulting record header
    * @return exists subquery operator
    */
  def planExistsSubQuery(lhs: K, rhs: K, targetField: Var, header: RecordHeader): K

  // Ternary operators

  /**
    * Performs a bounded variable length path expression.
    *
    * @param first          first previous operator
    * @param second         second previous operator
    * @param third          third previous operator
    * @param rel            relationship variable to expand from
    * @param edgeList       refers to the column in which the path is stored
    * @param target         node variable in the third input
    * @param initialEndNode initial end node
    * @param lower          lower bound
    * @param upper          upper bound
    * @param direction      path direction
    * @param header         resulting record header
    * @param isExpandInto   true, iff the target variable is solved and can be replaced by a filter
    * @return bounded var expand operator
    */
  def planBoundedVarExpand(
    first: K,
    second: K,
    third: K,
    rel: Var,
    edgeList: Var,
    target: Var,
    initialEndNode: Var,
    lower: Int,
    upper: Int,
    direction: Direction,
    header: RecordHeader,
    isExpandInto: Boolean
  ): K

  // N-ary operators
  /**
    * Performs a UNION ALL over graphs.
    *
    * @param graphs graphs to perform UNION ALL over together
    * @param qgn    name for the union graph
    * @return union all operator
    */
  def planGraphUnionAll(graphs: List[K], qgn: QualifiedGraphName): K
}
