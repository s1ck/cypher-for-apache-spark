package org.opencypher.spark.api.io.fs

import java.nio.file.Paths

import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.schema.Schema
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

  override def schema(name: GraphName): Option[Schema] = ???

  override def store(
    name: GraphName,
    graph: PropertyGraph
  ): Unit = ???

  override def delete(name: GraphName): Unit = ???

  override def graphNames: Set[GraphName] = ???
}


trait FileSystemAdapter {

  val rootPath: String

  def hasGraph(graphName: GraphName): Boolean =
    exists(graphPath(graphName))

  def readCAPSMetaData(graph: GraphName): CAPSGraphMetaData =
    CAPSGraphMetaData(readFile(graphPath(graph)))

  def writeCAPSMetaData(graph: GraphName, metaData: CAPSGraphMetaData): Unit =
    writeFile(graphPath(graph), metaData.asJson.toString())

  def readSchema(graph: GraphName): CAPSSchema =
    CAPSSchema(readFile(graphPath(graph)))

  def writeSchema(graph: GraphName, schema: CAPSSchema): Unit =
    writeFile(graphPath(graph), schema.asJson.toString)

  def readFile(path: String): String

  def writeFile(path: String, content: String): Unit

  def readTable: DataFrame

  def exists(path: String): Boolean

  private def graphPath(graphName: GraphName): String =
    Paths.get(rootPath, graphName.value).toString

}