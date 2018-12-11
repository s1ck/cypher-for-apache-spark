package org.opencypher.memcypher.impl.table

import org.opencypher.memcypher.impl.types.CypherTypeOps._
import org.opencypher.okapi.api.types.{CTFloat, CTInteger, CypherType}
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.util.ScalaUtils._
import org.opencypher.okapi.impl.util.TablePrinter
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.table.{Table => RelationalTable}
import org.opencypher.okapi.relational.impl.planning._
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.util.hashing.MurmurHash3

object Table {
  def empty: Table = Table(schema = Schema.empty, data = Seq.empty)

  def unit: Table = Table(schema = Schema.empty, data = Seq(Row()))
}

case class Table(schema: Schema, data: Seq[Row]) extends RelationalTable[Table] {

  private implicit val implicitSchema: Schema = schema

  override def select(cols: String*): Table = {
    val columnIndices = cols.map(schema.fieldIndex)

    val newSchema = schema.select(cols)
    val newLength = cols.length

    val newData = data.map { row =>
      val newValues = Array.ofDim[Any](newLength)
      for (i <- 0 until newLength) {
        newValues(i) = row.get(columnIndices(i))
      }
      Row(newValues)
    }
    Table(newSchema, newData)
  }

  override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): Table =
    copy(data = data.filter(_.eval[Boolean](expr).getOrElse(false)))

  override def drop(dropColumns: String*): Table =
    select(schema.columnNames.filterNot(dropColumns.contains): _*)

  override def join(
    other: Table,
    joinType: JoinType,
    joinCols: (String, String)*
  ): Table = joinType match {
    case InnerJoin => join(other, false, joinCols: _*)
    case RightOuterJoin => join(other, true, joinCols: _*)
    case LeftOuterJoin => other.join(this, true, joinCols.map { case (left, right) => right -> left }: _*)
    case CrossJoin => cartesian(other)
    case unsupported => throw UnsupportedOperationException(s"Join type '$unsupported' not supported.")
  }

  private def join(other: Table, rightOuter: Boolean, joinCols: (String, String)*): Table = {

    def hash(seq: Seq[Any]): Int = MurmurHash3.seqHash(seq)

    val (leftCols, rightCols) = joinCols.unzip
    val leftIndices = leftCols.map(schema.fieldIndex)
    val rightIndices = rightCols.map(other.schema.fieldIndex)

    val hashTable = data.map(row => hash(leftIndices.map(row.get)) -> row).groupBy(_._1)
    val emptyRow = Row(Array.ofDim[Any](schema.columns.length))

    val newData = other.data
      .filter(rightRow => rightOuter || hashTable.contains(hash(rightIndices.map(rightRow.get))))
      .flatMap(rightRow => {

        val rightValue = rightIndices.map(rightRow.get)
        hashTable.get(hash(rightValue)) match {

          case Some(leftValues) => leftValues
            .map(_._2)
            .filter(leftRow => leftIndices.map(leftRow.get) == rightValue) // hash collision check
            .map(leftRow => leftRow ++ rightRow)

          case None if rightOuter => Seq(emptyRow ++ rightRow)

          case None => Seq.empty[Row]
        }
      })

    copy(schema = schema ++ other.schema, data = newData)
  }

  private def cartesian(other: Table): Table =
    Table(schema = schema ++ other.schema, data = for {left <- data; right <- other.data} yield Row(left.values ++ right.values))

  override def unionAll(other: Table): Table = Table(schema, data = data ++ other.data)

  override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): Table = {
    import Row._
    import org.opencypher.memcypher.impl.types.CypherTypeOps._

    val sortItemsWithOrdering = sortItems.map {
      case (sortExpr, order) => (sortExpr, order, sortExpr.cypherType.ordering.asInstanceOf[Ordering[Any]])
    }

    object rowOrdering extends Ordering[Row] {
      override def compare(leftRow: Row, rightRow: Row): Int = {
        sortItemsWithOrdering.map { case (sortExpr, order, ordering) =>
          val leftValue = leftRow.evaluate(sortExpr)
          val rightValue = rightRow.evaluate(sortExpr)

          order match {
            case Ascending => ordering.compare(leftValue, rightValue)
            case Descending => ordering.reverse.compare(leftValue, rightValue)
          }
        }.collectFirst { case result if result != 0 => result }.getOrElse(0)
      }
    }

    copy(data = data.sorted(rowOrdering))
  }

  override def skip(n: Long): Table = copy(data = data.drop(n.toInt))

  override def limit(n: Long): Table = copy(data = data.take(n.toInt))

  override def distinct: Table = copy(data = data.distinct)

  override def group(by: Set[Var], aggregations: Set[(Aggregator, (String, CypherType))])
    (implicit header: RecordHeader, parameters: CypherMap): Table = {

    val byExprs = by.toSeq.sorted(Expr.alphabeticalOrdering)
    val byColumns = byExprs.map(header.column)

    val newSchema = Schema.empty
      .foldLeftOver(byExprs) {
        case (currentSchema, byExpr) => currentSchema.withColumn(header.column(byExpr), byExpr.cypherType)
      }
      .foldLeftOver(aggregations) {
        case (currentSchema, (_, (aggColumn, aggType))) => currentSchema.withColumn(aggColumn, aggType)
      }

    val groupedData = if (by.isEmpty) {
      Map(Seq.empty -> data)
    } else {
      data.groupBy(row => byColumns.map(schema.fieldIndex).map(row.get))
    }

    val newData = groupedData
      .mapValues { groupedRows =>
        aggregations.toSeq.map {
          case (aggregator, _) => aggregator match {
            case CountStar(_) =>
              groupedRows.size

            case Count(expr, distinct) if distinct =>
              groupedRows.map(_.evaluate(expr)).distinct.size

            case Count(expr, _) =>
              groupedRows.map(_.evaluate(expr)).size

            case Min(expr) =>
              groupedRows.map(_.evaluate(expr)).reduce(expr.cypherType.ordering.asInstanceOf[Ordering[Any]].min)

            case Max(expr) =>
              groupedRows.map(_.evaluate(expr)).reduce(expr.cypherType.ordering.asInstanceOf[Ordering[Any]].max)

            case Avg(expr) =>

              val sumFunc: (Any, Any) => Any = expr.cypherType.material match {
                case CTInteger => (x, y) => x.asInstanceOf[Long] + y.asInstanceOf[Long]
                case CTFloat   => (x, y) => x.asInstanceOf[Double] + y.asInstanceOf[Double]
                case other     => throw UnsupportedOperationException(s"CypherType $other not supported.")
              }

              val sum = groupedRows.map(_.evaluate(expr)).reduce[Any](sumFunc)

              expr.cypherType.material match {
                case CTInteger => sum.asInstanceOf[Long] / groupedRows.size.toDouble
                case CTFloat => sum.asInstanceOf[Double] / groupedRows.size.toDouble
                case other => throw UnsupportedOperationException(s"CypherType $other not supported.")
              }

            case Collect(expr, distinct) if distinct =>
              groupedRows.map(_.evaluate(expr)).distinct

            case Collect(expr, _) =>
              groupedRows.map(_.evaluate(expr))

            case other => throw UnsupportedOperationException(s"Aggregator $other not supported")
          }
        }
      }
      .map { case (groupValues, aggValues) => Row.fromSeq(groupValues ++ aggValues) }
      .toSeq

    Table(newSchema, newData)
  }

  override def withColumns(columns: (Expr, String)*)
    (implicit header: RecordHeader, parameters: CypherMap): Table = {

    val newSchema = columns.foldLeft(schema) {
      case (currentSchema, (expr, columnName)) => currentSchema.withColumn(columnName, expr.cypherType)
    }
    val newData = data.map(row => Row(row.values ++ columns.map { case (expr, _) => row.evaluate(expr) }))

    Table(newSchema, newData)
  }

  override def withColumnRenamed(oldColumn: String, newColumn: String): Table =
    copy(schema = schema.withColumnRenamed(oldColumn, newColumn))

  override def show(rows: Int): Unit =
    println(TablePrinter.toTable(schema.columns.map(_.name), data.take(rows).map(_.values.toSeq)))

  override def physicalColumns: Seq[String] =
    schema.columns.map(_.name)

  override def columnsFor(returnItem: String): Set[String] = ???

  override def columnType: Map[String, CypherType] =
    schema.columns.map(column => column.name -> column.dataType).toMap

  override def rows: Iterator[String => CypherValue] = ???

  override def size: Long = data.length
}






