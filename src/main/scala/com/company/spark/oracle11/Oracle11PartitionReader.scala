package com.company.spark.oracle11

import com.company.spark.oracle.oci.{OciConnection, OciResultReader, OciRow, OciStatement}

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
  private var connection: OciConnection = _
  private var statement: OciStatement = _
  private var resultReader: OciResultReader = _
  private var rowExtractor: OciRow => InternalRow = _
  private var currentRow: InternalRow = _

  // Reader lifecycle: open connection lazily on first `next`, then close all OCI resources in `close`.
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
      connection = Oracle11OciUtils.openConnection(options)
      statement = connection.prepareStatement(builtQuery.sql)
      Oracle11OciUtils.bindParameters(statement, builtQuery.params)
      resultReader = statement.executeQuery()
      rowExtractor = Oracle11OciUtils.buildReusableInternalRowExtractor(requiredSchema)
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
    if (resultReader.hasNext) {
      currentRow = rowExtractor(resultReader.next())
      true
    } else {
      currentRow = null
      false
    }
  }

  override def get(): InternalRow = currentRow

  override def close(): Unit = {
    Oracle11OciUtils.closeQuietly(resultReader)
    Oracle11OciUtils.closeQuietly(statement)
    Oracle11OciUtils.closeQuietly(connection)
    resultReader = null
    statement = null
    connection = null
    rowExtractor = null
    currentRow = null
    initialized = false
  }
}
