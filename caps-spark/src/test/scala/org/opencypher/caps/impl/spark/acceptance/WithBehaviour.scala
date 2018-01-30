/*
 * Copyright (c) 2016-2018 "Neo4j, Inc." [https://neo4j.com]
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
 */
package org.opencypher.caps.impl.spark.acceptance

import org.opencypher.caps.api.value.CAPSMap
import org.opencypher.caps.impl.spark.CAPSConverters._
import org.opencypher.caps.impl.spark.CAPSGraph

import scala.collection.Bag

trait WithBehaviour { this: AcceptanceTest =>

  def withBehaviour(initGraph: String => CAPSGraph): Unit = {
    test("rebinding of dropped variables") {
      // Given
      val given = initGraph("""CREATE (:Node {val: 1}), (:Node {val: 2})""")

      // When
      val result = given.cypher("""MATCH (n:Node)
          |WITH n.val AS foo
          |WITH foo + 2 AS bar
          |WITH bar + 2 AS foo
          |RETURN foo
        """.stripMargin)

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("foo" -> 5),
          CAPSMap("foo" -> 6)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("projecting constants") {
      // Given
      val given = initGraph("""CREATE (), ()""")

      // When
      val result = given.cypher("""MATCH ()
          |WITH 3 AS foo
          |WITH foo + 2 AS bar
          |RETURN bar
        """.stripMargin)

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("bar" -> 5),
          CAPSMap("bar" -> 5)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("projecting variables in scope") {

      // Given
      val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n, m RETURN n.val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("n.val" -> 4)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("projecting property expression") {

      // Given
      val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val AS n_val RETURN n_val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("n_val" -> 4)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("projecting property expression with filter") {

      // Given
      val given = initGraph("""CREATE (:Node {val: 3}), (:Node {val: 4}), (:Node {val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node) WITH n.val AS n_val WHERE n_val <= 4 RETURN n_val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("n_val" -> 3),
          CAPSMap("n_val" -> 4)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("projecting addition expression") {

      // Given
      val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node)-->(m:Node) WITH n.val + m.val AS sum_n_m_val RETURN sum_n_m_val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("sum_n_m_val" -> 9)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("aliasing variables") {

      // Given
      val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})""")

      // When
      val result = given.cypher("MATCH (n:Node)-[r]->(m:Node) WITH n.val + m.val AS sum WITH sum AS sum2 RETURN sum2")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("sum2" -> 9)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("projecting mixed expression") {

      // Given
      val given = initGraph("""CREATE (:Node {val: 4})-[:Rel]->(:Node {val: 5})-[:Rel]->(:Node)""")

      // When
      val result = given.cypher(
        "MATCH (n:Node)-[r]->(m:Node) WITH n.val AS n_val, n.val + m.val AS sum_n_m_val RETURN sum_n_m_val, n_val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("sum_n_m_val" -> 9, "n_val" -> 4),
          CAPSMap("sum_n_m_val" -> null, "n_val" -> 5)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("order by") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 3L),
          CAPSMap("val" -> 4L),
          CAPSMap("val" -> 42L)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("order by asc") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val ASC RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 3L),
          CAPSMap("val" -> 4L),
          CAPSMap("val" -> 42L)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("order by desc") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val DESC RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 42L),
          CAPSMap("val" -> 4L),
          CAPSMap("val" -> 3L)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val SKIP 2 RETURN val").asCaps

      // Then
      result.records.toDF().count() should equal(1)

      // And
      result.graphs shouldBe empty
    }

    test("order by with skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 4L),
          CAPSMap("val" -> 42L)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("order by with (arithmetic) skip") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 + 1 RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 42L)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val LIMIT 1 RETURN val").asCaps

      // Then
      result.records.toDF().count() should equal(1)

      // And
      result.graphs shouldBe empty
    }

    test("order by with limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val LIMIT 1 RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 3L)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("order by with (arithmetic) limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val LIMIT 1 + 1 RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 3L),
          CAPSMap("val" -> 4L)
        ))

      // And
      result.graphs shouldBe empty
    }

    test("order by with skip and limit") {
      val given = initGraph("""CREATE (:Node {val: 4}),(:Node {val: 3}),(:Node  {val: 42})""")

      val result = given.cypher("MATCH (a) WITH a.val as val ORDER BY val SKIP 1 LIMIT 1 RETURN val")

      // Then
      result.records.toMaps should equal(
        Bag(
          CAPSMap("val" -> 4L)
        ))

      // And
      result.graphs shouldBe empty
    }


    describe("NOT") {
      it("can project not of literal") {
        // Given
        val given = initGraph(
          """
            |CREATE ()
          """.stripMargin)

        // When
        val result = given.cypher(
          """
            |WITH true AS t, false AS f
            |WITH NOT true AS nt, not false AS nf
            |RETURN nt, nf""".stripMargin)

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("nt" -> false, "nf" -> true)
        ))
      }

      it("can project not of expression") {
        // Given
        val given = initGraph(
          """
            |CREATE ({id: 1, val: true}), ({id: 2, val: false})
          """.stripMargin)

        // When
        val result = given.cypher(
          """
            |MATCH (n)
            |WITH n.id AS id, NOT n.val AS val2
            |RETURN id, val2""".stripMargin)

        // Then
        result.records.toMaps should equal(Bag(
          CAPSMap("id" -> 1L, "val2" -> false),
          CAPSMap("id" -> 2L, "val2" -> true)
        ))
      }
    }
  }
}
