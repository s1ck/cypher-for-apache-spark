package org.opencypher.memcypher

import org.opencypher.memcypher.table.{MemTableRow, MemTableSchema}
import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.api.value.CypherValue.{CypherMap, CypherValue}
import org.opencypher.okapi.impl.exception.UnsupportedOperationException
import org.opencypher.okapi.impl.util.TablePrinter
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.relational.api.table.{Table => RelationalTable}
import org.opencypher.okapi.relational.impl.planning._
import org.opencypher.okapi.relational.impl.table.RecordHeader

import scala.util.hashing.MurmurHash3

object MemTable {
  def empty: MemTable = MemTable(schema = MemTableSchema.empty, data = Seq.empty)

  def unit: MemTable = MemTable(schema = MemTableSchema.empty, data = Seq(MemTableRow()))
}

case class MemTable(schema: MemTableSchema, data: Seq[MemTableRow]) extends RelationalTable[MemTable] {

  private implicit val implicitSchema: MemTableSchema = schema

  override def select(cols: String*): MemTable = {
    val renames = cols.map(c => c -> c)
    select(renames.head, renames.tail: _*)
  }

  override def select(col: (String, String), cols: (String, String)*): MemTable = {
    val renames = col +: cols
    val (oldCols, newCols) = renames.unzip

    val columnIndices = oldCols.map(schema.fieldIndex)

    val newSchema = renames.foldLeft(schema) {
      case (currentSchema, (oldCol, newCol)) => currentSchema.withColumnRenamed(oldCol, newCol)
    }.select(newCols)

    val newLength = renames.length

    val newData = data.map { row =>
      val newValues = Array.ofDim[Any](newLength)
      for (i <- 0 until newLength) {
        newValues(i) = row.get(columnIndices(i))
      }
      MemTableRow(newValues)
    }
    MemTable(newSchema, newData)
  }

  override def filter(expr: Expr)(implicit header: RecordHeader, parameters: CypherMap): MemTable =
    copy(data = data.filter(_.eval[Boolean](expr).getOrElse(false)))

  override def drop(dropColumns: String*): MemTable =
    select(schema.columnNames.filterNot(dropColumns.contains): _*)

  override def join(
    other: MemTable,
    joinType: JoinType,
    joinCols: (String, String)*
  ): MemTable = joinType match {
    case InnerJoin => join(other, false, joinCols: _*)
    case RightOuterJoin => join(other, true, joinCols: _*)
    case LeftOuterJoin => other.join(this, true, joinCols.map { case (left, right) => right -> left }: _*)
    case CrossJoin => cartesian(other)
    case unsupported => throw UnsupportedOperationException(s"Join type '$unsupported' not supported.")
  }

  private def join(other: MemTable, rightOuter: Boolean, joinCols: (String, String)*): MemTable = {

    def hash(seq: Seq[Any]): Int = MurmurHash3.seqHash(seq)

    val (leftCols, rightCols) = joinCols.unzip
    val leftIndices = leftCols.map(schema.fieldIndex)
    val rightIndices = rightCols.map(other.schema.fieldIndex)

    val hashTable = data.map(row => hash(leftIndices.map(row.get)) -> row).groupBy(_._1)
    val emptyRow = MemTableRow(Array.ofDim[Any](schema.columns.length))

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

          case None => Seq.empty[MemTableRow]
        }
      })

    copy(schema = schema ++ other.schema, data = newData)
  }

  private def cartesian(other: MemTable): MemTable =
    MemTable(schema = schema ++ other.schema, data = for {left <- data; right <- other.data} yield MemTableRow(left.values ++ right.values))

  override def unionAll(other: MemTable): MemTable = MemTable(schema, data = data ++ other.data)

  override def orderBy(sortItems: (Expr, Order)*)(implicit header: RecordHeader, parameters: CypherMap): MemTable = {
    import MemTableRow._
    import org.opencypher.memcypher.types.CypherTypeOps._

    val sortItemsWithOrdering = sortItems.map {
      case (sortExpr, order) => (sortExpr, order, sortExpr.cypherType.ordering.asInstanceOf[Ordering[Any]])
    }

    object rowOrdering extends Ordering[MemTableRow] {
      override def compare(leftRow: MemTableRow, rightRow: MemTableRow): Int = {
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

  override def skip(n: Long): MemTable = copy(data = data.drop(n.toInt))

  override def limit(n: Long): MemTable = copy(data = data.take(n.toInt))

  override def distinct: MemTable = copy(data = data.distinct)

  override def group(by: Set[Var], aggregations: Map[String, Aggregator])
    (implicit header: RecordHeader, parameters: CypherMap): MemTable = {

//    val byExprs = by.toSeq.sorted(Expr.alphabeticalOrdering)
//    val byColumns = byExprs.map(header.column)
//
//    val newSchema = Schema.empty
//      .foldLeftOver(byExprs) {
//        case (currentSchema, byExpr) => currentSchema.withColumn(header.column(byExpr), byExpr.cypherType)
//      }
//      .foldLeftOver(aggregations) {
//        case (currentSchema, (_, (aggColumn, aggType))) => currentSchema.withColumn(aggColumn, aggType)
//      }
//
//    val groupedData = if (by.isEmpty) {
//      Map(Seq.empty -> data)
//    } else {
//      data.groupBy(row => byColumns.map(schema.fieldIndex).map(row.get))
//    }
//
//    val newData = groupedData
//      .mapValues { groupedRows =>
//        aggregations.toSeq.map {
//          case (aggregator, _) => aggregator match {
//            case _: CountStar =>
//              groupedRows.size
//
//            case Count(expr, distinct) if distinct =>
//              groupedRows.map(_.evaluate(expr)).distinct.size
//
//            case Count(expr, _) =>
//              groupedRows.map(_.evaluate(expr)).size
//
//            case Min(expr) =>
//              groupedRows.map(_.evaluate(expr)).reduce(expr.cypherType.ordering.asInstanceOf[Ordering[Any]].min)
//
//            case Max(expr) =>
//              groupedRows.map(_.evaluate(expr)).reduce(expr.cypherType.ordering.asInstanceOf[Ordering[Any]].max)
//
//            case Avg(expr) =>
//
//              val sumFunc: (Any, Any) => Any = expr.cypherType.material match {
//                case CTInteger => (x, y) => x.asInstanceOf[Long] + y.asInstanceOf[Long]
//                case CTFloat   => (x, y) => x.asInstanceOf[Double] + y.asInstanceOf[Double]
//                case other     => throw UnsupportedOperationException(s"CypherType $other not supported.")
//              }
//
//              val sum = groupedRows.map(_.evaluate(expr)).reduce[Any](sumFunc)
//
//              expr.cypherType.material match {
//                case CTInteger => sum.asInstanceOf[Long] / groupedRows.size.toDouble
//                case CTFloat => sum.asInstanceOf[Double] / groupedRows.size.toDouble
//                case other => throw UnsupportedOperationException(s"CypherType $other not supported.")
//              }
//
//            case Collect(expr, distinct) if distinct =>
//              groupedRows.map(_.evaluate(expr)).distinct
//
//            case Collect(expr, _) =>
//              groupedRows.map(_.evaluate(expr))
//
//            case other => throw UnsupportedOperationException(s"Aggregator $other not supported")
//          }
//        }
//      }
//      .map { case (groupValues, aggValues) => Row.fromSeq(groupValues ++ aggValues) }
//      .toSeq
//
//    MemTable(newSchema, newData)
    this
  }

  override def withColumns(columns: (Expr, String)*)
    (implicit header: RecordHeader, parameters: CypherMap): MemTable = {

    val newSchema = columns.foldLeft(schema) {
      case (currentSchema, (expr, columnName)) => currentSchema.withColumn(columnName, expr.cypherType)
    }
    val newData = data.map(row => MemTableRow(row.values ++ columns.map { case (expr, _) => row.evaluate(expr) }))

    MemTable(newSchema, newData)
  }

  override def show(rows: Int): Unit =
    println(TablePrinter.toTable(schema.columns.map(_.name), data.take(rows).map(_.values.toSeq)))

  override def physicalColumns: Seq[String] =
    schema.columns.map(_.name)

  override def columnType: Map[String, CypherType] =
    schema.columns.map(column => column.name -> column.dataType).toMap

  override def rows: Iterator[String => CypherValue] = ???

  override def size: Long = data.length
}






