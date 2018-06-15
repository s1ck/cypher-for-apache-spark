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
package org.opencypher.okapi.logical.impl.logical

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, _}
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, _}
import org.opencypher.okapi.ir.impl.IrTestSuite
import org.opencypher.okapi.ir.test.support.MatchHelper._
import org.opencypher.okapi.logical.impl._

import scala.language.implicitConversions

class LogicalOptimizerTest extends IrTestSuite {

  val emptySqm = SolvedQueryModel.empty
  val logicalGraph = LogicalCatalogGraph(testQualifiedGraphName, Schema.empty)
  val schema = Schema.empty

  //  //Helper to create nicer expected results with `asCode`
  //  import org.opencypher.caps.impl.common.AsCode._
  //  implicit val specialMappings = Map[Any, String](
  //    schema -> "schema",
  //    emptySqm -> "emptySqm",
  //    logicalGraph -> "logicalGraph",
  //    emptySqm -> "emptySqm",
  //    (CTNode: CTNode) -> "CTNode"
  //  )

  def plannerContext(schema: Schema) =
    LogicalPlannerContext(schema, Set.empty, Map(testNamespace -> testGraphSource(testGraphName -> schema)))

  it("pushes label filter into scan") {
    val animalSchema = schema.withNodePropertyKeys("Animal")()
    val animalGraph = LogicalCatalogGraph(testQualifiedGraphName, animalSchema)
    val query =
      """|MATCH (a:Animal)
         |RETURN a""".stripMargin
    val plan = logicalPlan(query, animalSchema)
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(animalSchema))

    val expected = Select(
      List(Var("a")(CTNode(Set("Animal")))),
      NodeScan(
        Var("a")(CTNode(Set("Animal"))),
        animalGraph,
        SolvedQueryModel(Set(IRField("a")(CTNode)), Set(HasLabel(Var("a")(CTNode(Set("Animal"))), Label("Animal"))(CTBoolean)))
      ),
      SolvedQueryModel(Set(IRField("a")(CTNode)), Set(HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  it("rewrites missing label scan to empty records") {
    val query =
      """|MATCH (a:Animal)
         |RETURN a""".stripMargin
    val plan = logicalPlan(query, schema)
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(schema))

    val expected = Select(
      List(Var("a")(CTNode(Set("Animal")))),
      Empty(
        Set(Var("a")(CTNode(Set("Animal")))),
        logicalGraph,
        SolvedQueryModel(Set(IRField("a")(CTNode)), Set(HasLabel(Var("a")(CTNode(Set("Animal"))), Label("Animal"))(CTBoolean)))
      ),
      SolvedQueryModel(Set(IRField("a")(CTNode)), Set(HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  it("rewrites missing label combination") {
    val query =
      """|MATCH (a:Animal:Astronaut)
         |RETURN a""".stripMargin
    val schema = Schema.empty.withNodePropertyKeys("Animal")().withNodePropertyKeys("Astronaut")()
    val logicalGraph = LogicalCatalogGraph(testQualifiedGraphName, schema)

    val plan = logicalPlan(query, schema)
    val optimizedLogicalPlan = LogicalOptimizer(plan)(plannerContext(schema))

    val expected = Select(
      List(Var("a")(CTNode(Set("Animal", "Astronaut")))),
      Empty(
        Set(Var("a")(CTNode(Set("Astronaut", "Animal")))),
        logicalGraph,
        SolvedQueryModel(
          Set(IRField("a")(CTNode)),
          Set(
            HasLabel(Var("a")(CTNode(Set("Astronaut", "Animal"))), Label("Astronaut"))(CTBoolean),
            HasLabel(Var("a")(CTNode(Set("Astronaut", "Animal"))), Label("Animal"))(CTBoolean)
          )
        )
      ),
      SolvedQueryModel(
        Set(IRField("a")(CTNode)),
        Set(
          HasLabel(Var("a")(CTNode), Label("Animal"))(CTBoolean),
          HasLabel(Var("a")(CTNode), Label("Astronaut"))(CTBoolean)))
    )

    optimizedLogicalPlan should equalWithTracing(expected)
  }

  private def logicalPlan(query: String, schema: Schema): LogicalOperator = {
    val producer = new LogicalOperatorProducer
    val logicalPlanner = new LogicalPlanner(producer)
    val ir = query.asCypherQuery(testGraphName -> schema)(schema)
    val logicalPlannerContext = plannerContext(schema)
    val logicalPlan = logicalPlanner(ir)(logicalPlannerContext)
    logicalPlan
  }

}
