package org.opencypher.memcypher.table

import org.opencypher.okapi.api.types.CypherType
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.impl.util.TablePrinter

object MemTableSchema {
  def empty: MemTableSchema = MemTableSchema(Array.empty)
}

case class MemTableSchema(columns: Array[ColumnSchema]) {

  def fieldIndex(name: String): Int = columns.map(_.name).indexOf(name)

  def dataType(name: String): CypherType = columns(fieldIndex(name)).dataType

  def withColumn(columnMeta: ColumnSchema): MemTableSchema = withColumn(columnMeta.name, columnMeta.dataType)

  def withColumn(name: String, dataType: CypherType): MemTableSchema = copy(columns = columns :+ ColumnSchema(name, dataType))

  def withColumnRenamed(oldName: String, newName: String): MemTableSchema = {
    val newColumns = columns.map {
      case columnMeta if columnMeta.name == oldName => ColumnSchema(newName, columnMeta.dataType)
      case columnMeta => columnMeta
    }
    copy(columns = newColumns)
  }

  def columnNames: Array[String] = columns.map(_.name)

  def select(names: Seq[String]): MemTableSchema = names.foldLeft(MemTableSchema.empty) {
    case (currentSchema, columnName) =>
      val columnMeta = columns
        .collectFirst { case colMeta if colMeta.name == columnName => colMeta }
        .getOrElse(columnNotFound(columnName))
      currentSchema.withColumn(columnMeta)
  }

  def ++(other: MemTableSchema): MemTableSchema = copy(columns = columns ++ other.columns)

  def pretty: String = TablePrinter.toTable(columns.map(_.name), Seq(columns.map(_.dataType).toSeq))

  private def columnNotFound(name: String): Nothing =
    throw IllegalArgumentException(expected = s"existing column (one of: ${columnNames.mkString("[", ", ", "]")})", name)
}

case class ColumnSchema(name: String, dataType: CypherType)