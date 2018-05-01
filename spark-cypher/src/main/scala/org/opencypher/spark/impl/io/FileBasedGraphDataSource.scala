package org.opencypher.spark.impl.io

import java.nio.file.Paths

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTRelationship
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

abstract class FileBasedGraphDataSource extends CAPSPropertyGraphDataSource {

  implicit val caps: CAPSSession
  val fs: FileSystemAdapter

  override def hasGraph(name: GraphName): Boolean =
    fs.hasGraph(name)

  override def graph(graphName: GraphName): PropertyGraph = {
    val schema: CAPSSchema = fs.readSchema(graphName)
    val capsMetaData: CAPSGraphMetaData = fs.readCAPSGraphMetaData(graphName)

    val nodeTables = schema.allLabelCombinations.map { combo =>
      val nonNullableProperties = schema.keysFor(Set(combo)).filterNot {
        case (_, cypherType) => cypherType.isNullable
      }.keySet
      val df = fs.readNodeTable(graphName, combo)
      val nonNullableColumns = nonNullableProperties + GraphEntity.sourceIdKey
      CAPSNodeTable(combo, df.setNonNullable(nonNullableColumns))
    }

    val relTables = schema.relationshipTypes.map { relType =>
      val nonNullableProperties = schema.relationshipKeys(relType).filterNot {
        case (_, cypherType) => cypherType.isNullable
      }.keySet
      val df = fs.readRelTable(graphName, relType)
      val nonNullableColumns = nonNullableProperties ++ Relationship.nonPropertyAttributes
      CAPSRelationshipTable(relType, df.setNonNullable(nonNullableColumns))
    }

    CAPSGraph.create(capsMetaData.tags, nodeTables.head, (nodeTables.tail ++ relTables).toSeq: _*)
  }

  override def schema(name: GraphName): Option[Schema] =
    Some(fs.readSchema(name))

  override def store(name: GraphName, graph: PropertyGraph): Unit = {
    val capsGraph = graph.asCaps
    val schema = capsGraph.schema
    fs.writeCAPSGraphMetaData(name, CAPSGraphMetaData(capsGraph.tags))
    fs.writeSchema(name, schema)

    schema.labelCombinations.combos.foreach { combo =>
      fs.writeNodeTable(name, combo, capsGraph.canonicalNodeTable(combo))
    }

    schema.relationshipTypes.foreach { relType =>
      fs.writeRelTable(name, relType, capsGraph.canonicalRelationshipTable(relType))
    }
  }

  override def delete(graphName: GraphName): Unit = fs.deleteGraph(graphName)

  override def graphNames: Set[GraphName] = fs.listGraphs
}


trait FileSystemAdapter {

  val rootPath: String

  protected def listDirectories(path: String): Set[String]

  protected def readFile(path: String): String

  protected def writeFile(path: String, content: String): Unit

  protected def readTable(path: String): DataFrame

  protected def writeTable(path: String, table: DataFrame): Unit

  protected def deleteDirectory(path: String): Unit

  def listGraphs: Set[GraphName] =
    listDirectories(rootPath).map(GraphName)

  def deleteGraph(graphName: GraphName): Unit =
    deleteDirectory(graphPath(graphName))

  def hasGraph(graphName: GraphName): Boolean =
    listGraphs.contains(graphName)

  def readCAPSGraphMetaData(graph: GraphName): CAPSGraphMetaData =
    CAPSGraphMetaData(readFile(graphPath(graph)))

  def writeCAPSGraphMetaData(graph: GraphName, metaData: CAPSGraphMetaData): Unit =
    writeFile(graphPath(graph), metaData.asJson.toString())

  def readSchema(graph: GraphName): CAPSSchema =
    CAPSSchema(readFile(graphPath(graph)))

  def writeSchema(graph: GraphName, schema: CAPSSchema): Unit =
    writeFile(graphPath(graph), schema.schema.asJson.toString)

  def writeNodeTable(graphName: GraphName, labels: Set[String], table: DataFrame): Unit =
    writeTable(graphName, nodePath(labels), table)

  def readNodeTable(graphName: GraphName, labels: Set[String]): DataFrame =
    readTable(graphName, nodePath(labels))

  def writeRelTable(graphName: GraphName, relType: String, table: DataFrame): Unit =
    writeTable(graphName, relPath(relType), table)

  def readRelTable(graphName: GraphName, relType: String): DataFrame =
    readTable(graphName, relPath(relType))

  private def writeTable(graphName: GraphName, path: String, table: DataFrame): Unit =
    writeTable(Paths.get(graphPath(graphName), path).toString, table)

  private def readTable(graphName: GraphName, path: String): DataFrame =
    readTable(Paths.get(graphPath(graphName), path).toString)

  private def graphPath(graphName: GraphName): String =
    Paths.get(rootPath, graphName.value).toString

  private def nodePath(labels: Set[String]): String =
    Paths.get("nodes", labels.toSeq.sorted.mkString("_")).toString

  private def relPath(relType: String): String =
    Paths.get("relationships", relType).toString

}

object CAPSGraphExport {

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
