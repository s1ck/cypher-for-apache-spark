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
package org.opencypher.memcypher.tck

import java.io.File

import org.opencypher.memcypher.{MemCypherSession, MemCypherTestSuite}
import org.opencypher.memcypher.support.creation.MemScanGraphFactory
import org.opencypher.okapi.tck.test.Tags.{BlackList, WhiteList}
import org.opencypher.okapi.tck.test.{ScenariosFor, TCKGraph}
import org.opencypher.okapi.testing.propertygraph.CypherTestGraphFactory
import org.opencypher.tools.tck.api.CypherTCK
import org.scalatest.Tag
import org.scalatest.prop.TableDrivenPropertyChecks._

import scala.io.Source
import scala.util.{Failure, Success, Try}

class TckMemCypherTest extends MemCypherTestSuite {

  object TckMemCypherTag extends Tag("TckMemCypher")

  // Defines the graphs to run on
  private val factories = Table(
    ("factory","additional_blacklist"),
    (MemScanGraphFactory, Set.empty[String])
  )

  private val defaultFactory: CypherTestGraphFactory[MemCypherSession] = MemScanGraphFactory

  private val failingBlacklist = getClass.getResource("/failing_blacklist").getFile
  private val temporalBlacklist = getClass.getResource("/temporal_blacklist").getFile
  private val wontFixBlacklistFile = getClass.getResource("/wont_fix_blacklist").getFile
  private val failureReportingBlacklistFile = getClass.getResource("/failure_reporting_blacklist").getFile
  private val scenarios = ScenariosFor(failingBlacklist, temporalBlacklist, wontFixBlacklistFile, failureReportingBlacklistFile)

  // white list tests are run on all factories
  forAll(factories) { (factory, additional_blacklist) =>
    forAll(scenarios.whiteList) { scenario =>
      if (!additional_blacklist.contains(scenario.toString)) {
        test(s"[${factory.name}, ${WhiteList.name}] $scenario", WhiteList, TckMemCypherTag, Tag(factory.name)) {
          scenario(TCKGraph(factory, memCypher.graphs.empty)).execute()
        }
      }
    }
  }

  // black list tests are run on default factory
  forAll(scenarios.blackList) { scenario =>
    test(s"[${defaultFactory.name}, ${BlackList.name}] $scenario", BlackList, TckMemCypherTag) {
      val tckGraph = TCKGraph(defaultFactory, memCypher.graphs.empty)

      Try(scenario(tckGraph).execute()) match {
        case Success(_) =>
          throw new RuntimeException(s"A blacklisted scenario actually worked: $scenario")
        case Failure(_) =>
          ()
      }
    }
  }

  it("computes the TCK coverage") {
    val failingScenarios = Source.fromFile(failingBlacklist).getLines().size
    val failingTemporalScenarios = Source.fromFile(temporalBlacklist).getLines().size
    val failureReportingScenarios = Source.fromFile(failureReportingBlacklistFile).getLines().size

    val allScenarios = scenarios.blacklist.size + scenarios.whiteList.size.toFloat
    val readOnlyScenarios = scenarios.whiteList.size + failingScenarios + failureReportingScenarios.toFloat + failingTemporalScenarios
    val smallReadOnlyScenarios = scenarios.whiteList.size + failingScenarios.toFloat

    val overallCoverage = scenarios.whiteList.size / allScenarios
    val readOnlyCoverage = scenarios.whiteList.size / readOnlyScenarios
    val smallReadOnlyCoverage = scenarios.whiteList.size / smallReadOnlyScenarios

    val report = s"""
      |TCK Coverage
      |------------
      |
      | Complete: ${overallCoverage * 100}%
      | Read Only: ${readOnlyCoverage * 100}%
      | Read Only (without Failure case Scenarios and temporal): ${smallReadOnlyCoverage * 100}%
    """.stripMargin

    println(report)
  }

  ignore("run custom scenario") {
    val file = new File(getClass.getResource("CustomTest.feature").toURI)

    CypherTCK
      .parseFilesystemFeature(file)
      .scenarios
      .foreach(scenario => scenario(TCKGraph(defaultFactory, memCypher.graphs.empty)).execute())
  }

  ignore("run single scenario") {
    scenarios.get("Ordering with aggregation")
      .foreach(scenario => scenario(TCKGraph(defaultFactory, memCypher.graphs.empty)).execute())
  }
}
