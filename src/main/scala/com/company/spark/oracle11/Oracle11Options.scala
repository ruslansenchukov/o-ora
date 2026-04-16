package com.company.spark.oracle11

import java.util.Locale

import scala.collection.JavaConverters._

import com.company.spark.common.BaseOptions
import org.apache.spark.sql.util.CaseInsensitiveStringMap

final case class Oracle11PartitioningOptions(
    partitionColumn: String,
    numPartitions: Int,
    lowerBound: Option[String],
    upperBound: Option[String],
    autoBounds: Boolean)
    extends Serializable {

  def isManualBounds: Boolean = !autoBounds
}

final case class Oracle11Options(
    url: String,
    user: String,
    password: String,
    dbtable: Option[String],
    query: Option[String],
    fetchSize: Int,
    connectTimeoutMs: Option[Int],
    readTimeoutMs: Option[Int],
    queryTimeoutSec: Option[Int],
    maxInListSize: Int,
    schemaCacheTtlSec: Int,
    autoPartitionMinRowsPerPartition: Long,
    partitioning: Option[Oracle11PartitioningOptions])
    extends Serializable {

  lazy val relation: Oracle11Relation = Oracle11Relation.fromOptions(this)

  lazy val baseOptions: BaseOptions =
    BaseOptions(
      url = url,
      user = user,
      password = password,
      dbtable = dbtable,
      query = query,
      fetchSize = fetchSize,
      partitionColumn = partitioning.map(_.partitionColumn),
      lowerBound = partitioning.flatMap(_.lowerBound),
      upperBound = partitioning.flatMap(_.upperBound),
      numPartitions = partitioning.map(_.numPartitions)
    )
}

object Oracle11Options {
  private val DefaultFetchSize = 1000
  private val DefaultMaxInListSize = 1000
  private val DefaultSchemaCacheTtlSec = 300
  private val DefaultAutoPartitionMinRowsPerPartition = 100000L

  def fromCaseInsensitive(options: CaseInsensitiveStringMap): Oracle11Options = {
    val data = Option(options)
      .map(_.asCaseSensitiveMap().asScala.toMap)
      .getOrElse(Map.empty[String, String])
    fromMap(data)
  }

  def fromJava(options: java.util.Map[String, String]): Oracle11Options = {
    val data = Option(options).map(_.asScala.toMap).getOrElse(Map.empty[String, String])
    fromMap(data)
  }

  def fromMap(raw: Map[String, String]): Oracle11Options = {
    val normalized = raw.collect {
      case (k, v) if v != null => k.toLowerCase(Locale.ROOT) -> v
    }

    def required(name: String, trim: Boolean = true): String = {
      val value = normalized.get(name).map(v => if (trim) v.trim else v)
      value.filter(_.nonEmpty).getOrElse {
        throw new IllegalArgumentException(s"Missing required option '$name'")
      }
    }

    def optional(name: String): Option[String] =
      normalized.get(name).map(_.trim).filter(_.nonEmpty)

    val url = required("url")
    val user = required("user")
    val password = required("password", trim = false)
    val dbtable = optional("dbtable")
    val query = optional("query")

    if (dbtable.isDefined == query.isDefined) {
      throw new IllegalArgumentException("Exactly one of 'dbtable' or 'query' must be provided")
    }

    val fetchSize = optional("fetchsize") match {
      case Some(value) =>
        val parsed = parsePositiveInt(value, "fetchsize")
        if (parsed <= 0) {
          throw new IllegalArgumentException(s"Option 'fetchsize' must be > 0, got $parsed")
        }
        parsed
      case None => DefaultFetchSize
    }

    val connectTimeoutMs = optional("connecttimeoutms").map(parsePositiveInt(_, "connectTimeoutMs"))
    val readTimeoutMs = optional("readtimeoutms").map(parsePositiveInt(_, "readTimeoutMs"))
    val queryTimeoutSec = optional("querytimeoutsec").map(parsePositiveInt(_, "queryTimeoutSec"))

    val maxInListSize = optional("maxinlistsize") match {
      case Some(value) => parsePositiveInt(value, "maxInListSize")
      case None        => DefaultMaxInListSize
    }

    val schemaCacheTtlSec = optional("schemacachettlsec") match {
      case Some(value) => parseInt(value, "schemaCacheTtlSec")
      case None        => DefaultSchemaCacheTtlSec
    }

    val autoPartitionBounds = optional("autopartitionbounds")
      .map(parseBoolean(_, "autoPartitionBounds"))
      .getOrElse(false)

    val autoPartitionMinRowsPerPartition = optional("autopartitionminrowsperpartition") match {
      case Some(value) => parsePositiveLong(value, "autoPartitionMinRowsPerPartition")
      case None        => DefaultAutoPartitionMinRowsPerPartition
    }

    val partitionColumn = optional("partitioncolumn")
    val lowerBound = optional("lowerbound")
    val upperBound = optional("upperbound")
    val numPartitions = optional("numpartitions")

    val partitioning = {
      if (autoPartitionBounds) {
        val parsedPartitions = numPartitions
          .map(parsePositiveInt(_, "numPartitions"))
          .getOrElse {
            throw new IllegalArgumentException(
              "Option 'autoPartitionBounds=true' requires 'numPartitions'")
          }

        val column = partitionColumn.getOrElse {
          throw new IllegalArgumentException(
            "Option 'autoPartitionBounds=true' requires 'partitionColumn'")
        }

        if (lowerBound.isDefined || upperBound.isDefined) {
          throw new IllegalArgumentException(
            "Options 'lowerBound' and 'upperBound' must not be provided when 'autoPartitionBounds=true'")
        }

        Some(
          Oracle11PartitioningOptions(
            partitionColumn = column,
            numPartitions = parsedPartitions,
            lowerBound = None,
            upperBound = None,
            autoBounds = true
          ))
      } else {
        val items = Seq(partitionColumn, lowerBound, upperBound, numPartitions)
        if (items.exists(_.isDefined) && !items.forall(_.isDefined)) {
          throw new IllegalArgumentException(
            "Options 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions' must be provided together")
        }

        if (items.forall(_.isDefined)) {
          Some(
            Oracle11PartitioningOptions(
              partitionColumn = partitionColumn.get,
              numPartitions = parsePositiveInt(numPartitions.get, "numPartitions"),
              lowerBound = lowerBound,
              upperBound = upperBound,
              autoBounds = false
            ))
        } else {
          None
        }
      }
    }

    Oracle11Options(
      url = url,
      user = user,
      password = password,
      dbtable = dbtable,
      query = query,
      fetchSize = fetchSize,
      connectTimeoutMs = connectTimeoutMs,
      readTimeoutMs = readTimeoutMs,
      queryTimeoutSec = queryTimeoutSec,
      maxInListSize = maxInListSize,
      schemaCacheTtlSec = schemaCacheTtlSec,
      autoPartitionMinRowsPerPartition = autoPartitionMinRowsPerPartition,
      partitioning = partitioning
    )
  }

  private def parseInt(value: String, name: String): Int = {
    try {
      value.toInt
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"Option '$name' must be an integer, got '$value'")
    }
  }

  private def parsePositiveInt(value: String, name: String): Int = {
    val parsed = parseInt(value, name)
    if (parsed <= 0) {
      throw new IllegalArgumentException(s"Option '$name' must be > 0, got $parsed")
    }
    parsed
  }

  private def parsePositiveLong(value: String, name: String): Long = {
    val parsed =
      try {
        value.toLong
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"Option '$name' must be a long integer, got '$value'")
      }

    if (parsed <= 0L) {
      throw new IllegalArgumentException(s"Option '$name' must be > 0, got $parsed")
    }
    parsed
  }

  private def parseBoolean(value: String, name: String): Boolean = {
    value.trim.toLowerCase(Locale.ROOT) match {
      case "true"  => true
      case "false" => false
      case _ =>
        throw new IllegalArgumentException(s"Option '$name' must be true or false, got '$value'")
    }
  }
}
