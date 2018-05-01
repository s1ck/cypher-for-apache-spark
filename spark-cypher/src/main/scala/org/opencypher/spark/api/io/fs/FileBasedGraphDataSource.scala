package org.opencypher.spark.api.io.fs

import java.nio.file.Paths

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.spark.api.io.{CAPSNodeTable, CAPSRelationshipTable, GraphEntity, Relationship}
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.CAPSGraph
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.impl.io.hdfs.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema

abstract class FileBasedGraphDataSource extends CAPSPropertyGraphDataSource {

  val fs: FileSystemAdapter

  override def hasGraph(name: GraphName): Boolean =
    fs.hasGraph(name)

  override def graph(graphName: GraphName): PropertyGraph = {
    val schema: CAPSSchema = fs.readSchema(graphName)

    val capsMetaData = fs.readCAPSMetaData(graphName)

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

    // graph meta data
    fs.writeCAPSMetaData(name, CAPSGraphMetaData(capsGraph.tags))

    // schema
    fs.writeSchema(name, schema)

    // nodes
    schema.labelCombinations.combos.foreach { combo =>
      val nodes = capsGraph.nodesWithExactLabels("n", combo)
      fs.writeNodeTable(name, combo, nodes.data)
    }

    // rels
    schema.relationshipTypes.foreach { relType =>
      val rels = capsGraph.relationships("r", CTRelationship(relType))
      fs.writeRelTable(name, relType, rels.data)
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

  def readCAPSMetaData(graph: GraphName): CAPSGraphMetaData =
    CAPSGraphMetaData(readFile(graphPath(graph)))

  def writeCAPSMetaData(graph: GraphName, metaData: CAPSGraphMetaData): Unit =
    writeFile(graphPath(graph), metaData.asJson.toString())

  def readSchema(graph: GraphName): CAPSSchema =
    CAPSSchema(readFile(graphPath(graph)))

  def writeSchema(graph: GraphName, schema: CAPSSchema): Unit =
    writeFile(graphPath(graph), schema.asJson.toString)

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
