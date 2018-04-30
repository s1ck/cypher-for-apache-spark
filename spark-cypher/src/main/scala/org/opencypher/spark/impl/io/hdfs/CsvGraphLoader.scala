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
package org.opencypher.spark.impl.io.hdfs

import java.net.URI
import java.nio.file.Paths

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.opencypher.okapi.api.graph.PropertyGraph
import org.opencypher.okapi.api.io.conversion.{NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.exception.{GraphNotFoundException, InvalidGraphException}
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable}
import org.opencypher.spark.impl.DataFrameOps._
import CsvGraphLoader._

/**
  * Loads a graph stored in indexed CSV format from HDFS or the local file system
  * The CSV files must be stored following this schema:
  * # Nodes
  *   - all files describing nodes are stored in a subfolder called "nodes"
  *   - create one file for each possible label combination that exists in the data, i.e. there must not be overlapping
  * entities in different files (e.g. all nodes with labels :Person:Employee in a single file and all nodes that
  * have label :Person exclusively in another file)
  *   - for every node csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the node schema file see [[CsvNodeSchema]]
  * # Relationships
  *   - all files describing relationships are stored in a subfolder called "relationships"
  *   - create one csv file per relationship type
  *   - for every relationship csv file create a schema file called FILE_NAME.csv.SCHEMA
  *   - for information about the structure of the relationship schema file see [[CsvRelSchema]]
  *
  * @param fileHandler CsvGraphLoaderFileHandler file handler for hdfs or local file system
  * @param capsSession CAPS Session
  */
class CsvGraphLoader(fileHandler: CsvFileHandler)(implicit capsSession: CAPSSession) {

  private val sparkSession: SparkSession = capsSession.sparkSession

  def load: PropertyGraph = {

    if (!fileHandler.exists(fileHandler.graphLocation.getPath)) {
      throw GraphNotFoundException(s"CSV graph with name '${fileHandler.graphLocation.getPath}'")
    }
    if (!fileHandler.exists(Paths.get(fileHandler.graphLocation.getPath, NODES_DIRECTORY).toString)) {
      throw InvalidGraphException(s"CSV graph is missing required directory '$NODES_DIRECTORY'")
    }

    val nodeTables = loadNodes

    val relTables = if (fileHandler.exists(Paths.get(fileHandler.graphLocation.getPath, RELS_DIRECTORY).toString)) {
      loadRels
    } else {
      List.empty[CAPSRelationshipTable]
    }

    val metaData = if (fileHandler.exists(Paths.get(fileHandler.graphLocation.getPath, NODES_DIRECTORY).toString)) {
      loadMetaData
    } else {
      CAPSGraphMetaData.empty
    }
    capsSession.readFrom(metaData.tags, nodeTables.head, nodeTables.tail ++ relTables: _*)
  }

  private def loadMetaData: CAPSGraphMetaData =
    fileHandler.readMetaData(METADATA_FILE)

  private def loadNodes: List[CAPSNodeTable] = {
    val csvFiles = fileHandler.listNodeFiles.toList

    csvFiles.map(e => {
      val schema = parseSchema(e)(CsvNodeSchema(_))

      val userDefinedSchema = schema.toStructType

      val intermediateDF = sparkSession.read
        // Spark does not respect the nullable member in StructField
        .schema(userDefinedSchema)
        .csv(e.toString)

      val dataFrame = convertLists(intermediateDF, schema)

      val nodeMapping = NodeMapping.create(schema.idField.name,
        impliedLabels = schema.implicitLabels.toSet,
        optionalLabels = schema.optionalLabels.map(_.name).toSet,
        propertyKeys = schema.propertyFields.map(_.name).toSet)

      val nonNullableFields = userDefinedSchema.fields.filterNot(_.nullable)
      val dfWithCorrectNullability = nonNullableFields.foldLeft(dataFrame) {
        case (currentDF, structField) => currentDF.setNonNullable(structField.name)
      }

      CAPSNodeTable(nodeMapping, dfWithCorrectNullability)
    })
  }

  private def loadRels: List[CAPSRelationshipTable] = {
    val csvFiles = fileHandler.listRelationshipFiles.toList

    csvFiles.map(relationShipFile => {

      val schema = parseSchema(relationShipFile)(CsvRelSchema(_))

      val userDefinedSchema = schema.toStructType

      val intermediateDF = sparkSession.read
        .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSS")
        // Spark does not respect the nullable member in StructField
        .schema(userDefinedSchema)
        .csv(relationShipFile.toString)

      val dataFrame = convertLists(intermediateDF, schema)

      val relMapping = RelationshipMapping.create(schema.idField.name,
        schema.startIdField.name,
        schema.endIdField.name,
        schema.relationshipType,
        schema.propertyFields.map(_.name).toSet)

      val nonNullableFields = userDefinedSchema.fields.filterNot(_.nullable)
      val dfWithCorrectNullability = nonNullableFields.foldLeft(dataFrame) {
        case (currentDF, structField) => currentDF.setNonNullable(structField.name)
      }

      CAPSRelationshipTable(relMapping, dfWithCorrectNullability)
    })
  }

  private def parseSchema[T <: CsvSchema](path: URI)(parser: String => T): T = {
    val text = fileHandler.readSchemaFile(path)
    parser(text)
  }

  /**
    * CSV does not allow to read array fields. Thus we have to read all list fields as Strings and then manually convert
    * them
    *
    * @param dataFrame the input data frame with list column represented as string
    * @param schema    the dataframes csv schema
    * @return Date frame with array list columns
    */
  private def convertLists(dataFrame: DataFrame, schema: CsvSchema): DataFrame = {
    schema.propertyFields
      .filter(field => field.getTargetType.isInstanceOf[ArrayType])
      .foldLeft(dataFrame) {
        case (df, field) =>
          df.safeReplaceColumn(field.name, functions.split(df(field.name), "\\|").cast(field.getTargetType))
      }
  }
}

object CsvGraphLoader {

  val NODES_DIRECTORY = "nodes"

  val RELS_DIRECTORY = "relationships"

  val CSV_SUFFIX = ".CSV"

  val SCHEMA_SUFFIX = ".SCHEMA"

  val METADATA_FILE = "metadata.json"

  def apply(location: URI, hadoopConfig: Configuration)(implicit caps: CAPSSession): CsvGraphLoader = {
    new CsvGraphLoader(new HadoopFileHandler(location, hadoopConfig))
  }

  def apply(location: URI)(implicit caps: CAPSSession): CsvGraphLoader = {
    new CsvGraphLoader(new LocalFileHandler(location))
  }
}
