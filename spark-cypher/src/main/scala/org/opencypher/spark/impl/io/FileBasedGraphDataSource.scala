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
package org.opencypher.spark.impl.io

import java.nio.file.Paths

import io.circe.Decoder.Result
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.{LabelPropertyMap, RelTypePropertyMap, Schema}
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.okapi.impl.exception.GraphNotFoundException
import org.opencypher.okapi.impl.schema.SchemaImpl
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.impl.table.ColumnName
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable, GraphEntity, Relationship}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.io.CAPSGraphExport._
import org.opencypher.spark.impl.io.hdfs.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema

import scala.util.Try

abstract class FileBasedGraphDataSource extends CAPSPropertyGraphDataSource {

  implicit val session: CAPSSession
  val fs: FileSystemAdapter

  protected var schemaCache: Map[GraphName, CAPSSchema] = Map.empty

  override def hasGraph(graphName: GraphName): Boolean = {
    schemaCache.contains(graphName) || fs.hasGraph(graphName)
  }

  override def graph(graphName: GraphName): PropertyGraph = {
    Try {
      val capsSchema: CAPSSchema = schema(graphName).get
      val capsMetaData: CAPSGraphMetaData = fs.readCAPSGraphMetaData(graphName)

      val nodeTables = capsSchema.allLabelCombinations.map { combo =>
        val nonNullableProperties = capsSchema.keysFor(Set(combo)).filterNot {
          case (_, cypherType) => cypherType.isNullable
        }.keySet
        val nonNullableColumns = nonNullableProperties + GraphEntity.sourceIdKey
        val df = fs.readNodeTable(graphName, combo, capsSchema.canonicalNodeTableSchema(combo))
        CAPSNodeTable(combo, df.setNonNullable(nonNullableColumns))
      }

      val relTables = capsSchema.relationshipTypes.map { relType =>
        val nonNullableProperties = capsSchema.relationshipKeys(relType).filterNot {
          case (_, cypherType) => cypherType.isNullable
        }.keySet
        val nonNullableColumns = nonNullableProperties ++ Relationship.nonPropertyAttributes
        val df = fs.readRelTable(graphName, relType, capsSchema.canonicalRelTableSchema(relType))
        CAPSRelationshipTable(relType, df.setNonNullable(nonNullableColumns))
      }

      CAPSGraph.create(capsMetaData.tags, nodeTables.head, (nodeTables.tail ++ relTables).toSeq: _*)
    }.toOption.getOrElse(throw GraphNotFoundException(s"sGraph with name '$graphName'"))
  }

  override def schema(graphName: GraphName): Option[CAPSSchema] = {
    if (schemaCache.contains(graphName)) {
      schemaCache.get(graphName)
    } else {
      val s = fs.readSchema(graphName)
      schemaCache += graphName -> s
      Some(s)
    }
  }

  override def store(graphName: GraphName, graph: PropertyGraph): Unit = {
    val capsGraph = graph.asCaps
    val schema = capsGraph.schema
    schemaCache += graphName -> schema
    fs.writeCAPSGraphMetaData(graphName, CAPSGraphMetaData(capsGraph.tags))
    fs.writeSchema(graphName, schema)

    schema.labelCombinations.combos.foreach { combo =>
      fs.writeNodeTable(graphName, combo, capsGraph.canonicalNodeTable(combo))
    }

    schema.relationshipTypes.foreach { relType =>
      fs.writeRelTable(graphName, relType, capsGraph.canonicalRelationshipTable(relType))
    }
  }

  override def delete(graphName: GraphName): Unit = {
    schemaCache -= graphName
    fs.deleteGraph(graphName)
  }

  override def graphNames: Set[GraphName] = fs.listGraphs
}

trait FileSystemAdapter {

  import CAPSSchema._
  import JsonSerialization._
  import io.circe.syntax._

  val rootPath: String

  protected def listDirectories(path: String): Set[String]

  protected def deleteDirectory(path: String): Unit

  protected def readFile(path: String): String

  protected def writeFile(path: String, content: String): Unit

  protected def readTable(path: String, schema: StructType): DataFrame

  protected def writeTable(path: String, table: DataFrame): Unit

  def listGraphs: Set[GraphName] =
    listDirectories(rootPath).map(GraphName)

  def deleteGraph(graphName: GraphName): Unit =
    deleteDirectory(graphPath(graphName))

  def hasGraph(graphName: GraphName): Boolean =
    listGraphs.contains(graphName)

  def readCAPSGraphMetaData(graph: GraphName): CAPSGraphMetaData =
    parse[CAPSGraphMetaData](readFile(capsMetaDataPath(graph)))

  def writeCAPSGraphMetaData(graph: GraphName, metaData: CAPSGraphMetaData): Unit =
    writeFile(capsMetaDataPath(graph), metaData.asJson.toString())

  def readSchema(graph: GraphName): CAPSSchema = {
    val schemaString = readFile(schemaPath(graph))
    parse[Schema](schemaString).asCaps
  }

  def writeSchema(graph: GraphName, schema: CAPSSchema): Unit =
    writeFile(schemaPath(graph), schema.schema.asJson.toString)

  def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit =
    writeTable(graphName, nodePath(labels), table)

  def readNodeTable(graphName: GraphName, labels: Set[String], schema: StructType): DataFrame =
    readTable(graphName, nodePath(labels), schema: StructType)

  def writeRelTable(graphName: GraphName, relType: String, table: DataFrame): Unit =
    writeTable(graphName, relPath(relType), table)

  def readRelTable(graphName: GraphName, relType: String, schema: StructType): DataFrame =
    readTable(graphName, relPath(relType), schema)

  private def writeTable(graphName: GraphName, path: String, table: DataFrame): Unit =
    writeTable(Paths.get(graphPath(graphName), path).toString, table)

  private def readTable(graphName: GraphName, path: String, schema: StructType): DataFrame =
    readTable(Paths.get(graphPath(graphName), path).toString, schema)

  private def capsMetaDataPath(graphName: GraphName): String = {
    Paths.get(rootPath, graphName.value, "capsMetaData.json").toString
  }

  private def schemaPath(graphName: GraphName): String = {
    Paths.get(rootPath, graphName.value, "schema.json").toString
  }

  private def graphPath(graphName: GraphName): String =
    Paths.get(rootPath, graphName.value).toString

  private def nodePath(labels: Set[String]): String =
    Paths.get("nodes", labels.toSeq.sorted.mkString("_")).toString

  private def relPath(relType: String): String =
    Paths.get("relationships", relType).toString

}

object JsonSerialization {

  import io.circe._
  import io.circe.generic.auto._
  import io.circe.generic.semiauto._

  def parse[A: Decoder](json: String): A = {
    parser.parse(json) match {
      case Right(parsedJson) => parsedJson.as[A].value
      case Left(f) => throw f // ParsingFailure
    }
  }

  implicit class DeserializationResult[A](r: Result[A]) {
    def value: A = {
      r match {
        case Right(value) => value
        case Left(f) => throw f // DecodingFailure
      }
    }
  }

  implicit val encodeLabelKeys: KeyEncoder[Set[String]] = new KeyEncoder[Set[String]] {
    override def apply(labels: Set[String]): String = labels.toSeq.sorted.mkString("_")
  }

  implicit val decodeLabelKeys: KeyDecoder[Set[String]] = new KeyDecoder[Set[String]] {
    override def apply(key: String): Option[Set[String]] = {
      if (key.isEmpty) Some(Set.empty) else Some(key.split("_").toSet)
    }
  }

  implicit val encodeSchema: Encoder[Schema] =
    Encoder.forProduct2("labelPropertyMap", "relTypePropertyMap")(s =>
      (s.labelPropertyMap.map, s.relTypePropertyMap.map)
    )

  implicit val decodeSchema: Decoder[Schema] =
    Decoder.forProduct2("labelPropertyMap", "relTypePropertyMap")(
      (lpm: Map[Set[String], PropertyKeys], rpm: Map[String, PropertyKeys]) =>
        SchemaImpl(LabelPropertyMap(lpm), RelTypePropertyMap(rpm)))

  implicit val encodeMetaData: Encoder[CAPSGraphMetaData] = deriveEncoder[CAPSGraphMetaData]
  implicit val decodeMetaData: Decoder[CAPSGraphMetaData] = deriveDecoder[CAPSGraphMetaData]

}

object CAPSGraphExport {

  implicit class CanonicalTableSparkSchema(val schema: Schema) extends AnyVal {

    import org.opencypher.spark.impl.convert.CAPSCypherType._

    def canonicalNodeTableSchema(labels: Set[String]): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val properties = schema.nodeKeys(labels).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: properties)
    }

    def canonicalRelTableSchema(relType: String): StructType = {
      val id = StructField(GraphEntity.sourceIdKey, LongType, nullable = false)
      val sourceId = StructField(Relationship.sourceStartNodeKey, LongType, nullable = false)
      val targetId = StructField(Relationship.sourceEndNodeKey, LongType, nullable = false)
      val properties = schema.relationshipKeys(relType).toSeq.sortBy(_._1).map { case (propertyName, cypherType) =>
        StructField(propertyName, cypherType.getSparkType, cypherType.isNullable)
      }
      StructType(id +: sourceId +: targetId +: properties)
    }
  }

  implicit class CanonicalTableExport(graph: CAPSGraph) {

    def canonicalNodeTable(labels: Set[String]): DataFrame = {
      val varName = "n"
      val nodeRecords = graph.nodesWithExactLabels(varName, labels)

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val propertyRenamings = nodeRecords.header.propertySlots(Var(varName)())
        .map { case (p, slot) => ColumnName.of(slot) -> p.key.name }

      val selectColumns = (idRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => nodeRecords.data.col(oldName).as(newName)
      }

      nodeRecords.data.select(selectColumns: _*)
    }

    def canonicalRelationshipTable(relType: String): DataFrame = {
      val varName = "r"
      val relCypherType = CTRelationship(relType)
      val v = Var(varName)(relCypherType)

      val relRecords = graph.relationships(varName, relCypherType)

      val idRenaming = varName -> GraphEntity.sourceIdKey
      val sourceIdRenaming = ColumnName.of(relRecords.header.sourceNodeSlot(v)) -> Relationship.sourceStartNodeKey
      val targetIdRenaming = ColumnName.of(relRecords.header.targetNodeSlot(v)) -> Relationship.sourceEndNodeKey
      val propertyRenamings = relRecords.header.propertySlots(Var(varName)())
        .map { case (p, slot) => ColumnName.of(slot) -> p.key.name }

      val selectColumns = (idRenaming :: sourceIdRenaming :: targetIdRenaming :: propertyRenamings.toList.sorted).map {
        case (oldName, newName) => relRecords.data.col(oldName).as(newName)
      }

      relRecords.data.select(selectColumns: _*)
    }

  }

}
