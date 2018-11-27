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
package org.opencypher.spark.examples

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.opencypher.okapi.api.graph.Namespace
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.testing.utils.FileSystemUtils._
import org.opencypher.spark.util.LdbcUtil._
import org.opencypher.spark.util.{ConsoleApp, LdbcUtil}
/**
  * This demo reads data generated by the LDBC SNB data generator and performs the following steps:
  *
  * 1) Loads the raw CSV files into Hive tables
  * 2) Normalizes tables according to the LDBC schema (i.e. place -> [City, Country, Continent]
  * 3) Generates a Graph DDL script based on LDBC naming conventions (if not already existing)
  * 4) Initializes a SQL PGDS based on the generated Graph DDL file
  * 5) Runs a Cypher query over the LDBC graph in Spark
  *
  * More detail about the LDBC SNB data generator are available under https://github.com/ldbc/ldbc_snb_datagen
  */
object LdbcHiveExample extends ConsoleApp {

  implicit val resourceFolder: String = "/ldbc"
  val datasource = "warehouse"
  val database = "LDBC"

  implicit val session: CAPSSession = CAPSSession.local(CATALOG_IMPLEMENTATION.key -> "hive")
  implicit val spark: SparkSession = session.sparkSession

  val csvFiles = new File(resource("csv/").getFile).list()

  spark.sql(s"DROP DATABASE IF EXISTS $database CASCADE")
  spark.sql(s"CREATE DATABASE $database")

  // Load LDBC data from CSV files into Hive tables
  csvFiles.foreach { csvFile =>
    spark.read
      .format("csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .option("delimiter", "|")
      .load(resource(s"csv/$csvFile").getFile)
      // cast e.g. Timestamp to String
      .withCompatibleTypes
      .write
      .saveAsTable(s"$database.${csvFile.dropRight("_0_0.csv.gz".length)}")
  }

  // Create views that normalize LDBC data where necessary
  val views = readFile(resource("sql/ldbc_views.sql").getFile).split(";")
  views.foreach(spark.sql)

  // generate GraphDdl file
  val graphDdlString = LdbcUtil.toGraphDDL(datasource, database)
  writeFile(resource("ddl").getFile + "/ldbc.ddl", graphDdlString)

  // create SQL PGDS
  val sqlGraphSource = GraphSources
    .sql(resource("ddl/ldbc.ddl").getFile)
    .withSqlDataSourceConfigs(resource("ddl/data-sources.json").getFile)

  session.registerSource(Namespace("sql"), sqlGraphSource)

  session.cypher(
    s"""
       |FROM GRAPH sql.LDBC
       |MATCH (n:Person)-[:islocatedin]->(c:City)
       |RETURN n.firstName, c.name
       |ORDER BY n.firstName, c.name
       |LIMIT 20
     """.stripMargin).show
}
