package org.opencypher.spark.api

import org.neo4j.cypher.internal.frontend.v3_2.SemanticState
import org.neo4j.cypher.internal.frontend.v3_2.parser.CypherParser

class Frontend {

  private val parser = new CypherParser

  def parse(query: String) = {
    val statement = parser.parse(query)
  }

}
