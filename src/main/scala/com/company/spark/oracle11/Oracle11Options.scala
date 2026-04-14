package com.company.spark.oracle11

import java.util.Locale

import scala.collection.JavaConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

final case class Oracle11PartitioningOptions(
    partitionColumn: String,
    lowerBound: String,
    upperBound: String,
    numPartitions: Int)
    extends Serializable

final case class Oracle11Options(
    url: String,
    user: String,
    password: String,
    dbtable: Option[String],
    query: Option[String],
    fetchSize: Int,
    partitioning: Option[Oracle11PartitioningOptions])
    extends Serializable {

  lazy val relation: Oracle11Relation = Oracle11Relation.fromOptions(this)
}

object Oracle11Options {
  private val DefaultFetchSize = 1000

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
        val parsed = parseInt(value, "fetchsize")
        if (parsed <= 0) {
          throw new IllegalArgumentException(s"Option 'fetchsize' must be > 0, got $parsed")
        }
        parsed
      case None => DefaultFetchSize
    }

    val partitionColumn = optional("partitioncolumn")
    val lowerBound = optional("lowerbound")
    val upperBound = optional("upperbound")
    val numPartitions = optional("numpartitions")

    val partitioning = {
      val items = Seq(partitionColumn, lowerBound, upperBound, numPartitions)
      if (items.exists(_.isDefined) && !items.forall(_.isDefined)) {
        throw new IllegalArgumentException(
          "Options 'partitionColumn', 'lowerBound', 'upperBound', and 'numPartitions' must be provided together")
      }

      if (items.forall(_.isDefined)) {
        val parsedPartitions = parseInt(numPartitions.get, "numPartitions")
        if (parsedPartitions <= 0) {
          throw new IllegalArgumentException(s"Option 'numPartitions' must be > 0, got $parsedPartitions")
        }

        Some(
          Oracle11PartitioningOptions(
            partitionColumn = partitionColumn.get,
            lowerBound = lowerBound.get,
            upperBound = upperBound.get,
            numPartitions = parsedPartitions
          ))
      } else {
        None
      }
    }

    Oracle11Options(
      url = url,
      user = user,
      password = password,
      dbtable = dbtable,
      query = query,
      fetchSize = fetchSize,
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
}
