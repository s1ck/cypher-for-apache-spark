package org.opencypher.okapi.relational.impl.planning

object PropertyGraphStatistics {
  val empty = PropertyGraphStatistics(Map.empty, Map.empty)
}

case class PropertyGraphStatistics(nodeCounts: Map[Set[String], Long], relCounts: Map[String, Long])
