package org.opencypher.caps.cosc.impl.planning

import java.net.URI

import org.opencypher.caps.api.exception.{IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.caps.api.graph.PropertyGraph
import org.opencypher.caps.api.value.CypherValue.CypherMap
import org.opencypher.caps.cosc.impl.COSCRecords
import org.opencypher.caps.impl.flat._
import org.opencypher.caps.ir.api.util.DirectCompilationStage
import org.opencypher.caps.logical.impl.LogicalExternalGraph

case class COSCPlannerContext(
  resolver: URI => PropertyGraph,
  records: COSCRecords,
  parameters: CypherMap)

class COSCPlanner extends DirectCompilationStage[FlatOperator, COSCOperator, COSCPlannerContext]{

  override def process(input: FlatOperator)(implicit context: COSCPlannerContext): COSCOperator = {
    input match {

      case Project(expr, in, header) =>
        COSCProject(expr, process(in), header)

      case Filter(expr, in, header) =>
        COSCFilter(expr, process(in), header)

      case Select(fields, graphs, in, header) =>
        COSCSelect(fields, graphs, process(in), header)

      case op@NodeScan(node, in, header) =>
        COSCScan(process(in), op.sourceGraph, node, header)

      case SetSourceGraph(graph, in, _) => graph match {
        case g: LogicalExternalGraph =>
          COSCSetSourceGraph(process(in), g)

        case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
      }

      case Start(graph, _) => graph match {
        case g: LogicalExternalGraph =>
          COSCStart(context.records, g)
        case _ => throw IllegalArgumentException("a LogicalExternalGraph", graph)
      }

      case other => throw UnsupportedOperationException(s"No implementation for Flat operator $other")
    }
  }
}