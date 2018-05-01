package org.opencypher.spark.api.io.csv.file

import org.junit.rules.TemporaryFolder
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.api.io.PropertyGraphDataSource
import org.opencypher.okapi.testing.propertygraph.TestGraph
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.CAPSPGDSAcceptance
import org.opencypher.spark.test.CAPSTestSuite
import org.opencypher.spark.test.support.creation.caps.CAPSScanGraphFactory

class FileCsvDataSourceAcceptance extends CAPSTestSuite with CAPSPGDSAcceptance {

  private var tempDir = new TemporaryFolder()

  override def initSession(): CAPSSession = caps

  override protected def beforeEach(): Unit = {
    tempDir.create()
    super.beforeEach()
  }

  override protected def afterEach(): Unit = {
    tempDir.delete()
    tempDir = new TemporaryFolder()
    super.afterEach()
  }

  override def create(graphName: GraphName, testGraph: TestGraph, createStatements: String): PropertyGraphDataSource = {
    val propertyGraph = CAPSScanGraphFactory(testGraph)

    // DO NOT DELETE THIS! If the config is not cleared, the FileCsvPGDSAcceptanceTest fails because of connecting to a
    // non-existent HDFS cluster. We could not figure out why the afterAll call in MiniDFSClusterFixture does not handle
    // the clearance of the config correctly.
    session.sparkContext.hadoopConfiguration.clear()
    val ds = FileCsvDataSource(tempDir.getRoot.getAbsolutePath)
    ds.store(graphName, propertyGraph)
    ds
  }

}
