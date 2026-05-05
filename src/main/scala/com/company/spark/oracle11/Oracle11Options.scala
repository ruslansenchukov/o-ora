package com.company.spark.oracle11

import java.util.Locale

import scala.collection.JavaConverters._

import com.company.spark.oracle.oci.OciConnection

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
    connectString: String,
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

    def value(name: String, trim: Boolean = true): Option[String] =
      normalized
        .get(name)
        .map(v => if (trim) v.trim else v)
        .filter(_.nonEmpty)

    def requiredAny(names: Seq[String], trim: Boolean = true): String = {
      val maybe = names.iterator.flatMap(name => value(name, trim)).toSeq.headOption
      maybe.getOrElse {
        throw new IllegalArgumentException(s"Missing required option. Provide one of: ${names.mkString(", ")}")
      }
    }

    def optional(name: String): Option[String] = value(name, trim = true)

    def optionalAny(names: Seq[String], trim: Boolean = true): Option[String] =
      names.iterator.flatMap(name => value(name, trim)).toSeq.headOption

    val user = requiredAny(Seq("oracle.user", "user"))
    val password = requiredAny(Seq("oracle.password", "password"), trim = false)

    val urlOption = optionalAny(Seq("oracle.url", "url"))
    val hostOption = optional("oracle.host")
    val portOption = optional("oracle.port")
    val serviceNameOption = optional("oracle.servicename")
    val sidOption = optional("oracle.sid")

    if (serviceNameOption.isDefined && sidOption.isDefined) {
      throw new IllegalArgumentException(
        "Options 'oracle.serviceName' and 'oracle.sid' are mutually exclusive")
    }

    val (url, connectString) = urlOption match {
      case Some(rawUrl) =>
        rawUrl -> toConnectString(rawUrl)
      case None =>
        val host = hostOption.getOrElse {
          throw new IllegalArgumentException(
            "Missing required option. Provide 'oracle.url' (or 'url') or native host options " +
              "'oracle.host' + 'oracle.port' + ('oracle.serviceName' or 'oracle.sid')")
        }
        val port = portOption
          .map(parsePositiveInt(_, "oracle.port"))
          .getOrElse {
            throw new IllegalArgumentException("Missing required option 'oracle.port'")
          }

        if (serviceNameOption.isEmpty && sidOption.isEmpty) {
          throw new IllegalArgumentException(
            "One of 'oracle.serviceName' or 'oracle.sid' must be provided when using host/port options")
        }

        val connect = OciConnection.buildConnectString(host, port, serviceNameOption, sidOption)
        val logical = serviceNameOption
          .map(s => s"oci://$host:$port/service/$s")
          .orElse(sidOption.map(s => s"oci://$host:$port/sid/$s"))
          .get

        logical -> connect
    }

    val dbtable = optional("dbtable")
    val query = optional("query")

    if (dbtable.isDefined == query.isDefined) {
      throw new IllegalArgumentException("Exactly one of 'dbtable' or 'query' must be provided")
    }

    val fetchSize = optional("fetchsize") match {
      case Some(valueRaw) =>
        val parsed = parsePositiveInt(valueRaw, "fetchsize")
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
      case Some(valueRaw) => parsePositiveInt(valueRaw, "maxInListSize")
      case None           => DefaultMaxInListSize
    }

    val schemaCacheTtlSec = optional("schemacachettlsec") match {
      case Some(valueRaw) => parseInt(valueRaw, "schemaCacheTtlSec")
      case None           => DefaultSchemaCacheTtlSec
    }

    val autoPartitionBounds = optional("autopartitionbounds")
      .map(parseBoolean(_, "autoPartitionBounds"))
      .getOrElse(false)

    if (autoPartitionBounds) {
      throw new IllegalArgumentException(
        "Option 'autoPartitionBounds=true' is not supported in native OCI mode (v1). " +
          "Use manual bounds: partitionColumn + lowerBound + upperBound + numPartitions")
    }

    val autoPartitionMinRowsPerPartition = optional("autopartitionminrowsperpartition") match {
      case Some(valueRaw) => parsePositiveLong(valueRaw, "autoPartitionMinRowsPerPartition")
      case None           => DefaultAutoPartitionMinRowsPerPartition
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

    Oracle11Options(
      url = url,
      connectString = connectString,
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

  private def toConnectString(rawUrl: String): String = {
    val trimmed = rawUrl.trim
    val jdbcPrefix = "jdbc:oracle:thin:@"

    if (!trimmed.toLowerCase(Locale.ROOT).startsWith(jdbcPrefix)) {
      return trimmed
    }

    val tail = trimmed.substring(jdbcPrefix.length)
    if (tail.startsWith("(") || tail.startsWith("//")) {
      return tail
    }

    val hostPortSid = "^([^:]+):(\\d+):(.+)$".r
    tail match {
      case hostPortSid(host, port, sid) =>
        OciConnection.buildConnectString(host, port.toInt, None, Some(sid))
      case _ =>
        tail
    }
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
