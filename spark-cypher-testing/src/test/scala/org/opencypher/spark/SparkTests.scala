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
package org.opencypher.spark

import org.apache.spark.sql.execution.SparkPlan
import org.opencypher.okapi.impl.util.Measurement
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintPhysicalPlan
import org.opencypher.spark.testing.CAPSTestSuite
import org.opencypher.spark.testing.fixture.GraphConstructionFixture

import scala.io.StdIn

class SparkTests extends CAPSTestSuite with GraphConstructionFixture {

//  it("caches entity scans") {
////    PrintPhysicalPlan.set
//
//    val graph = initGraph(
//      """
//        |CREATE (a:Person { name: 'Alice' })
//        |CREATE (b:Person { name: 'Bob' })
//        |CREATE (c:Book { name: '1984' })
//        |CREATE (a)-[e:KNOWS]->(b)
//      """.stripMargin)
//
//    val query =
//      """
//        |MATCH (a:Person)-[:KNOWS]->(b:Person)
//        |RETURN a.name, b.name
//      """.stripMargin
//
//    val result = graph.cypher(query)
//
//    import org.opencypher.spark.impl.CAPSConverters._
//
//
//
//    println(result.getRecords.asCaps.df.queryExecution.executedPlan)
//
//    result.show
//
//    StdIn.readLine()
//
//  }

  // Example for: https://issues.apache.org/jira/browse/SPARK-23855
  ignore("should correctly perform a join after a cross") {
    val df1 = sparkSession.createDataFrame(Seq(Tuple1(0L)))
      .toDF("a")

    val df2 = sparkSession.createDataFrame(Seq(Tuple1(1L)))
      .toDF("b")

    val df3 = sparkSession.createDataFrame(Seq(Tuple1(0L)))
      .toDF("c")

    val cross = df1.crossJoin(df2)
    cross.show()

    val joined = cross
      .join(df3, cross.col("a") === df3.col("c"))

    joined.show()

    val selected = joined.select("*")
    selected.show
  }

  ignore("generates Spark SQL plans and measures planning and execution costs") {
    import sparkSession.implicits._

    val stageMetrics = ch.cern.sparkmeasure.StageMetrics(sparkSession)


    val minLength = 10
    val maxLength = 15

    val timings = (minLength to maxLength).foldLeft(Seq.empty[(SparkPlan, Long, Long, Long, Long, Long)]) {
      (results, i) =>

        val resultDF = (1 to i).foldLeft(sparkSession.createDataFrame(Seq(Tuple1(0L))).toDF("a0")) {
          case (df, j) if j % 5 == 0 =>
            df.select($"a${j - 1}".as(s"a$j"))
          case (df, j) if j % 5 == 1 =>
            df.join(df.withColumnRenamed(s"a${j - 1}", s"a$j"), $"a${j - 1}" === $"a$j")
              .drop(s"a${j - 1}")
          case (df, j) if j % 5 == 2 =>
            df.union(df).withColumnRenamed(s"a${j - 1}", s"a$j")
          case (df, j) if j % 5 == 3 =>
            df.join(sparkSession.createDataFrame(Seq(Tuple1(0))).toDF(s"a$j"), $"a${j - 1}" === $"a$j")
          case (df, j) if j % 5 == 4 =>
            df.distinct().withColumnRenamed(s"a${j - 1}", s"a$j")

        }

        val ((sparkPlan, sparkPlanPlanningTime, executedPlanPlanningTime, beginSnapshot, endSnapshot), totalTime) = Measurement.time {
          // spark plan planning time
          val (_, sparkPlanPlanningTime) = Measurement.time(resultDF.queryExecution.sparkPlan)

          // executed plan planning time
          val (sparkPlan, executedPlanPlanningTime) = Measurement.time(resultDF.queryExecution.executedPlan)

          // execution
          val beginSnapshot = stageMetrics.begin()
          resultDF.show()
          val endSnapshot = stageMetrics.end()
          (sparkPlan, sparkPlanPlanningTime, executedPlanPlanningTime, beginSnapshot, endSnapshot)
        }

        stageMetrics.createStageMetricsDF()
        val aggregateDf = sparkSession.sql(s"SELECT " +
          s"MIN(submissionTime) AS minSubmissionTime, " +
          s"MAX(completionTime) - MIN(submissionTime) AS elapsedTime " +
          s"FROM PerfStageMetrics " +
          s"WHERE submissionTime >= $beginSnapshot AND completionTime <= $endSnapshot")

        val aggregateValues = aggregateDf.take(1)(0).toSeq
        val cols = aggregateDf.columns
        val sqlTime = (cols zip aggregateValues).collectFirst { case (n: String, v: Long) if n == "elapsedTime" => v }.get
        val submissionTime = (cols zip aggregateValues).collectFirst { case (n: String, v: Long) if n == "minSubmissionTime" => v }.get

        val unknownTime = submissionTime - beginSnapshot

        val run = (sparkPlan, sparkPlanPlanningTime, executedPlanPlanningTime, sqlTime, totalTime, unknownTime)
        results :+ run
    }

    println(timings
      .zip(minLength to maxLength)
      .map {
        case ((_, sparkPlanPlanningTime, executedPlanPlanningTime, sqlTime, totalTime, unknownTime), numOperators) =>
          (numOperators, sparkPlanPlanningTime, executedPlanPlanningTime, sqlTime, totalTime, unknownTime)
      })

    // uncomment to keep Spark UI running for investigation
    //    StdIn.readLine()
  }
}
