package com.company.spark.oracle11

import java.sql.{Connection, PreparedStatement, ResultSet}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

final class Oracle11PartitionReader(
    options: Oracle11Options,
    relation: Oracle11Relation,
    requiredSchema: StructType,
    partitionPredicate: Option[Oracle11SqlPredicate],
    pushedPredicate: Option[Oracle11SqlPredicate])
    extends PartitionReader[InternalRow] {

  private var initialized = false
  private var connection: Connection = _
  private var statement: PreparedStatement = _
  private var resultSet: ResultSet = _
  private var currentRow: InternalRow = _

  // Reader lifecycle: open connection lazily on first `next`, then close all JDBC resources in `close`.
  private def initializeIfNeeded(): Unit = {
    if (initialized) {
      return
    }

    val builtQuery = Oracle11QueryBuilder.buildSelect(
      relation = relation,
      requiredSchema = requiredSchema,
      partitionPredicate = partitionPredicate,
      pushedPredicate = pushedPredicate,
      limit = None
    )

    try {
      connection = Oracle11JdbcUtils.openConnection(options)
      statement = connection.prepareStatement(builtQuery.sql)
      statement.setFetchSize(options.fetchSize)
      Oracle11JdbcUtils.bindParameters(statement, builtQuery.params)
      resultSet = statement.executeQuery()
      initialized = true
    } catch {
      case t: Throwable =>
        close()
        throw new RuntimeException(
          s"Failed to execute reader query for ${relation.description}. SQL=[${builtQuery.sql}]. ${t.getMessage}",
          t)
    }
  }

  override def next(): Boolean = {
    initializeIfNeeded()
    if (resultSet.next()) {
      currentRow = Oracle11JdbcUtils.toInternalRow(resultSet, requiredSchema)
      true
    } else {
      currentRow = null
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    Oracle11JdbcUtils.closeQuietly(resultSet)
    Oracle11JdbcUtils.closeQuietly(statement)
    Oracle11JdbcUtils.closeQuietly(connection)
    resultSet = null
    statement = null
    connection = null
    currentRow = null
    initialized = false
  }
}
