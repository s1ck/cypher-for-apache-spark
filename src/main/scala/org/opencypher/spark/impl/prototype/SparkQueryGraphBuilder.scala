package org.opencypher.spark.impl.prototype

import org.neo4j.cypher.internal.frontend.v3_2.ast
import org.neo4j.cypher.internal.frontend.v3_2.ast._

import scala.collection.mutable

class SparkQueryGraphBuilder {
  val predicates: mutable.Set[Expression] = mutable.Set.empty
  val nodes: mutable.Set[String] = mutable.Set.empty
  val rels: mutable.Set[Expression] = mutable.Set.empty
  val returns: mutable.Set[Expression] = mutable.Set.empty

  def add(c: Clause) = {
    c match {
      case Match(_, pattern, _, where) =>
        add(pattern)
        where.foreach(w => addPredicate(w.expression))
      case Return(_, ReturnItems(_, items), _, _, _, _) =>
        items.foreach(addReturn)
    }
  }

  def addReturn(r: ReturnItem) = {
    r match {
      case AliasedReturnItem(expr, variable) => returns.add(expr)
      case UnaliasedReturnItem(expr, text) => returns.add(expr)
    }
  }

  def add(p: Pattern) = {

  }

  def addPredicate(e: ast.Expression) = {

  }

  def build(): SparkQueryGraph = {

    new SparkQueryGraph {
      override def variables(): Set[Variable] = ???

      override def labels(): Set[Label] = ???

      override def relTypes(): Set[RelType] = ???

      override def propertyKeys(): Set[PropertyKey] = ???

      override def block(): Block = ???
    }
  }
}

object SparkQueryGraph {
  def from(s: Statement): SparkQueryGraph = {
    val builder = new SparkQueryGraphBuilder()
    s match {
      case Query(_, part) => part match {
        case SingleQuery(clauses) => clauses.foreach(builder.add)
      }
      case _ => ???
    }

    builder.build()
  }
}
