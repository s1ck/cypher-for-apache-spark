package org.opencypher.spark.impl.prototype

trait SparkQueryGraph {
  def variables(): Set[Variable]
  def labels(): Set[Label]
  def relTypes(): Set[RelType]
  def propertyKeys(): Set[PropertyKey]

  def block(): Block
}

trait Block {
  def blockType(): BlockType
  def dependent(): Option[Block]
  def given(): Set[Entities]
  def predicates(): Set[Predicate]
}

trait Predicate
case class Connected(source: Node, rel: Relationship, target: Node) extends Predicate
case class HasLabel(node: Node, label: Label) extends Predicate
case class HasType(rel: Relationship, relType: RelType) extends Predicate
case class Equals(lhs: Expression, rhs: Expression) extends Predicate

trait Expression
case class Property(node: Node, key: PropertyKey) extends Expression
trait Literal extends Expression
case class IntegerLiteral(value: Long) extends Literal

trait Node {
  def variable: Variable
}
trait Relationship

trait Label
trait RelType
trait PropertyKey
trait Variable

sealed trait Entities
case object Nodes extends Entities
case object Relationships extends Entities

sealed trait BlockType
case object OptionalBlock extends BlockType
case object StandardBlock extends BlockType
