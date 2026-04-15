package com.company.spark.oracle11

import java.sql.{Connection, PreparedStatement, ResultSet}

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.types.StructType

final class Oracle11PartitionReader(
    options: Oracle11Options,
    relation: Oracle11Relation,
    requiredSchema: StructType,
    partitionPredicate: Option[Oracle11SqlPredicate],
    pushedPredicate: Option[Oracle11SqlPredicate],
    pushedLimit: Option[Int])
    extends PartitionReader[InternalRow] {

  private var initialized = false
  private var connection: Connection = _
  private var statement: PreparedStatement = _
  private var resultSet: ResultSet = _
  private var rowExtractor: ResultSet => InternalRow = _
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
      limit = pushedLimit
    )

    try {
      connection = Oracle11JdbcUtils.openConnection(options)
      tuneConnection(connection)

      statement = connection.prepareStatement(builtQuery.sql)
      statement.setFetchSize(options.fetchSize)
      safely {
        statement.setFetchDirection(ResultSet.FETCH_FORWARD)
      }
      options.queryTimeoutSec.foreach(statement.setQueryTimeout)

      Oracle11JdbcUtils.bindParameters(statement, builtQuery.params)
      resultSet = statement.executeQuery()
      rowExtractor = Oracle11JdbcUtils.buildInternalRowExtractor(requiredSchema)
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
      currentRow = rowExtractor(resultSet)
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
    rowExtractor = null
    currentRow = null
    initialized = false
  }

  private def tuneConnection(conn: Connection): Unit = {
    safely {
      conn.setReadOnly(true)
    }
    safely {
      conn.setAutoCommit(false)
    }
  }

  private def safely(operation: => Unit): Unit = {
    try {
      operation
    } catch {
      case NonFatal(_) =>
    }
  }
}
