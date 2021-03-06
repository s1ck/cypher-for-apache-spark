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
package org.opencypher.okapi.ir.impl

import org.opencypher.okapi.api.graph.{Namespace, PropertyGraph, QualifiedGraphName}
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.api.schema.PropertyGraphSchema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherMap
import org.opencypher.okapi.impl.graph.QGNGenerator
import org.opencypher.okapi.ir.api.block.SourceBlock
import org.opencypher.okapi.ir.api.expr.{Expr, Var}
import org.opencypher.okapi.ir.api.pattern.Pattern
import org.opencypher.okapi.ir.api.{IRCatalogGraph, IRField, IRGraph}
import org.opencypher.v9_0.ast.ViewInvocation
import org.opencypher.v9_0.ast.semantics.SemanticState
import org.opencypher.v9_0.util.InputPosition
import org.opencypher.v9_0.{expressions => ast}

final case class IRBuilderContext(
  qgnGenerator: QGNGenerator,
  queryString: String,
  parameters: CypherMap,
  workingGraph: IRGraph, // initially the ambient graph, but gets changed by `FROM GRAPH`/`CONSTRUCT`
  blockRegistry: BlockRegistry = BlockRegistry.empty,
  semanticState: SemanticState,
  queryLocalCatalog: QueryLocalCatalog, // copy of Session catalog plus constructed graph schemas
  // TODO: Unify instantiateView and queryCatalog into one abstraction that resolves graphs/views
  instantiateView: ViewInvocation => PropertyGraph,
  knownTypes: Map[ast.Expression, CypherType] = Map.empty) {
  self =>

  private lazy val exprConverter = new ExpressionConverter(self)
  private lazy val patternConverter = new PatternConverter(self)

  def convertPattern(p: ast.Pattern, qgn: Option[QualifiedGraphName] = None): Pattern = {
    patternConverter.convert(p, knownTypes, qgn.getOrElse(workingGraph.qualifiedGraphName))
  }

  def convertExpression(e: ast.Expression): Expr = exprConverter.convert(e)(lambdaVars = Map())

  def schemaFor(qgn: QualifiedGraphName): PropertyGraphSchema = queryLocalCatalog.schema(qgn)

  def withBlocks(reg: BlockRegistry): IRBuilderContext = copy(blockRegistry = reg)

  def withFields(fields: Set[IRField]): IRBuilderContext = {
    val withFieldTypes = fields.foldLeft(knownTypes) {
      case (acc, f) =>
        acc.updated(ast.Variable(f.name)(InputPosition.NONE), f.cypherType)
    }
    copy(knownTypes = withFieldTypes)
  }

  def withWorkingGraph(graph: IRGraph): IRBuilderContext =
    copy(workingGraph = graph)

  def registerGraph(qgn: QualifiedGraphName, graph: PropertyGraph): IRBuilderContext =
    copy(queryLocalCatalog = queryLocalCatalog.withGraph(qgn, graph))

  def registerSchema(qgn: QualifiedGraphName, schema: PropertyGraphSchema): IRBuilderContext =
    copy(queryLocalCatalog = queryLocalCatalog.withSchema(qgn, schema))

  def resetRegistry: IRBuilderContext = {
    val sourceBlock = SourceBlock(workingGraph)
    copy(blockRegistry = BlockRegistry.empty.register(sourceBlock))
  }
}

object IRBuilderContext {
  def initial(
    query: String,
    parameters: CypherMap,
    semState: SemanticState,
    workingGraph: IRCatalogGraph,
    qgnGenerator: QGNGenerator,
    sessionCatalog: Map[Namespace, PropertyGraphDataSource],
    instantiateView: ViewInvocation => PropertyGraph,
    fieldsFromDrivingTable: Set[Var] = Set.empty,
    queryCatalog: Map[QualifiedGraphName, PropertyGraph] = Map.empty
  ): IRBuilderContext = {
    val registry = BlockRegistry.empty
    val block = SourceBlock(workingGraph)
    val updatedRegistry = registry.register(block)
    val queryLocalCatalog = QueryLocalCatalog(sessionCatalog, queryCatalog)

    val context = IRBuilderContext(
      qgnGenerator,
      query,
      parameters,
      workingGraph,
      updatedRegistry,
      semState,
      queryLocalCatalog,
      instantiateView)

    context.withFields(fieldsFromDrivingTable.map(v => IRField(v.name)(v.cypherType)))
  }
}
