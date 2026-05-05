package com.company.spark.oracle11

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable.ArrayBuffer

import com.company.spark.oracle.oci.OciResultReader

import org.apache.spark.sql.types.{StructField, StructType}

object Oracle11SchemaInference {

  private final case class CachedSchema(schema: StructType, expiresAtMs: Long)
  private val schemaCache = new ConcurrentHashMap[String, CachedSchema]()

  // Schema inference intentionally uses metadata-only query (`WHERE 1=0`) to avoid full scans.
  def infer(options: Oracle11Options): StructType = inferWithLoader(options) {
    inferUncached(options)
  }

  private[oracle11] def inferWithLoader(options: Oracle11Options)(loader: => StructType): StructType = {
    val ttlSec = options.schemaCacheTtlSec
    if (ttlSec <= 0) {
      return loader
    }

    val key = cacheKey(options)
    val now = System.currentTimeMillis()
    val existing = schemaCache.get(key)

    if (existing != null && existing.expiresAtMs > now) {
      return existing.schema
    }

    if (existing != null && existing.expiresAtMs <= now) {
      schemaCache.remove(key, existing)
    }

    val loaded = loader
    val expiresAt = now + ttlSec.toLong * 1000L
    schemaCache.put(key, CachedSchema(loaded, expiresAt))
    loaded
  }

  private def inferUncached(options: Oracle11Options): StructType = {
    val relation = options.relation
    val sql = relation match {
      case Oracle11TableRelation(table) => s"SELECT * FROM $table WHERE 1 = 0"
      case Oracle11QueryRelation(query) => s"SELECT * FROM ($query) ORA11_SCHEMA WHERE 1 = 0"
    }

    val connection = Oracle11OciUtils.openConnection(options)
    var statement: com.company.spark.oracle.oci.OciStatement = null
    var result: OciResultReader = null

    try {
      statement = connection.prepareStatement(sql)
      result = statement.executeQuery()
      readStruct(result)
    } catch {
      case t: Throwable =>
        throw new IllegalArgumentException(
          s"Failed to infer schema for source [${relation.description}]: ${t.getMessage}",
          t)
    } finally {
      Oracle11OciUtils.closeQuietly(result)
      Oracle11OciUtils.closeQuietly(statement)
      Oracle11OciUtils.closeQuietly(connection)
    }
  }

  private def readStruct(result: OciResultReader): StructType = {
    val fields = new ArrayBuffer[StructField](result.columns.size)
    result.columns.foreach { col =>
      val sparkType = Oracle11TypeMapper.toSparkTypeFromOci(
        ociType = col.ociType.toInt,
        precision = col.precision,
        scale = col.scale,
        dataSize = col.dataSize
      )
      fields += StructField(col.name, sparkType, nullable = col.nullable)
    }
    StructType(fields.toSeq)
  }

  private def cacheKey(options: Oracle11Options): String = {
    val relationKey = options.relation.cacheKey
    s"${options.url}|${options.user}|$relationKey"
  }

  private[oracle11] def clearCacheForTests(): Unit = {
    schemaCache.clear()
  }
}
