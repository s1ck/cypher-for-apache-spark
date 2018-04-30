package org.opencypher.spark.api.io.fs

import java.nio.file.Paths

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.CTRelationship
import org.opencypher.spark.impl.CAPSConverters._
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.impl.io.hdfs.CAPSGraphMetaData
import org.opencypher.spark.schema.CAPSSchema

abstract class FileBasedGraphDataSource extends CAPSPropertyGraphDataSource {

  val fs: FileSystemAdapter

  override def hasGraph(name: GraphName): Boolean =
    fs.hasGraph(name)

  override def graph(name: GraphName): PropertyGraph = {
    ???
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

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] =
    fs.listGraphs
}


trait FileSystemAdapter {

  val rootPath: String

  def listDirectories(path: String): Set[String]

  def readFile(path: String): String

  def writeFile(path: String, content: String): Unit

  def readTable: DataFrame

  def writeTable(path: String, table: DataFrame): Unit

  def listGraphs: Set[GraphName] =
    listDirectories(rootPath).map(GraphName)

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
    writeTable(graphName, Paths.get("nodes", labels.toSeq.sorted.mkString("_")).toString, table)

  def writeRelTable(graphName: GraphName, relType: String, table: DataFrame): Unit =
    writeTable(graphName, Paths.get("relationships", relType).toString, table)

  def writeTable(graphName: GraphName, path: String, table: DataFrame): Unit =
    writeTable(Paths.get(graphPath(graphName), path).toString, table)

  private def graphPath(graphName: GraphName): String =
    Paths.get(rootPath, graphName.value).toString

}