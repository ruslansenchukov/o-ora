package com.company.spark.oracle11

import java.sql.ResultSetMetaData

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.types.{StructField, StructType}

object Oracle11SchemaInference {

  // Schema inference intentionally uses metadata-only query (`WHERE 1=0`) to avoid full scans.
  def infer(options: Oracle11Options): StructType = {
    val relation = options.relation
    val sql = relation match {
      case Oracle11TableRelation(table) => s"SELECT * FROM $table WHERE 1 = 0"
      case Oracle11QueryRelation(query) => s"SELECT * FROM ($query) ORA11_SCHEMA WHERE 1 = 0"
    }

    val connection = Oracle11JdbcUtils.openConnection(options)
    var statement: java.sql.PreparedStatement = null
    var resultSet: java.sql.ResultSet = null

    try {
      statement = connection.prepareStatement(sql)
      resultSet = statement.executeQuery()
      val meta = resultSet.getMetaData
      readStruct(meta)
    } catch {
      case t: Throwable =>
        throw new IllegalArgumentException(
          s"Failed to infer schema for source [${relation.description}]: ${t.getMessage}",
          t)
    } finally {
      Oracle11JdbcUtils.closeQuietly(resultSet)
      Oracle11JdbcUtils.closeQuietly(statement)
      Oracle11JdbcUtils.closeQuietly(connection)
    }
  }

  private def readStruct(meta: ResultSetMetaData): StructType = {
    val fields = new ArrayBuffer[StructField](meta.getColumnCount)
    var idx = 1
    while (idx <= meta.getColumnCount) {
      val name = Option(meta.getColumnLabel(idx)).filter(_.nonEmpty).getOrElse(meta.getColumnName(idx))
      val sparkType = Oracle11TypeMapper.toSparkType(
        jdbcType = meta.getColumnType(idx),
        precision = meta.getPrecision(idx),
        scale = meta.getScale(idx),
        typeName = meta.getColumnTypeName(idx)
      )
      val nullable = meta.isNullable(idx) != ResultSetMetaData.columnNoNulls
      fields += StructField(name, sparkType, nullable = nullable)
      idx += 1
    }

    StructType(fields.toSeq)
  }
}
