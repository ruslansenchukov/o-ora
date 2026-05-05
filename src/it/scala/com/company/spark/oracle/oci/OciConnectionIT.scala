package com.company.spark.oracle.oci

import org.scalatest.funsuite.AnyFunSuite

final case class OciItConfig(
    connectString: String,
    user: String,
    password: String)

final class OciConnectionIT extends AnyFunSuite {

  private val defaultQuery =
    "SELECT ID, AMOUNT, STATUS, CATEGORY FROM APP_USER.ORDERS_V2 WHERE ROWNUM <= 10"

  private def integrationEnabled: Boolean =
    sys.props.get("oracle11.it.enabled").exists(_.toBoolean)

  private def propOrEnv(prop: String, env: String): Option[String] =
    sys.props.get(prop).map(_.trim).filter(_.nonEmpty).orElse(sys.env.get(env).map(_.trim).filter(_.nonEmpty))

  private def resolveConfig(): Option[OciItConfig] = {
    val urlLike = propOrEnv("oracle.url", "ORACLE_URL")
      .orElse(propOrEnv("oracle.connectString", "ORACLE_CONNECT_STRING"))

    val host = propOrEnv("oracle.host", "ORACLE_HOST")
    val port = propOrEnv("oracle.port", "ORACLE_PORT").map(_.toInt)
    val serviceName = propOrEnv("oracle.serviceName", "ORACLE_SERVICE_NAME")
    val sid = propOrEnv("oracle.sid", "ORACLE_SID")
    val user = propOrEnv("oracle.user", "ORACLE_USER")
    val password = propOrEnv("oracle.password", "ORACLE_PASSWORD")

    val connectString = urlLike match {
      case Some(value) => Some(normalizeConnectString(value))
      case None =>
        for {
          h <- host
          p <- port
          if !(serviceName.isDefined && sid.isDefined)
          if serviceName.isDefined || sid.isDefined
        } yield OciConnection.buildConnectString(h, p, serviceName, sid)
    }

    for {
      c <- connectString
      u <- user
      p <- password
    } yield OciItConfig(c, u, p)
  }

  private def normalizeConnectString(value: String): String = {
    val trimmed = value.trim
    val jdbcPrefix = "jdbc:oracle:thin:@"
    if (!trimmed.toLowerCase.startsWith(jdbcPrefix)) {
      return trimmed
    }
    trimmed.substring(jdbcPrefix.length)
  }

  private def requireConfig(): OciItConfig = {
    if (!integrationEnabled) {
      cancel("Set -Doracle11.it.enabled=true to run OCI integration tests.")
    }

    resolveConfig().getOrElse {
      cancel(
        "Missing OCI integration target. Provide either oracle.url (or oracle.connectString), " +
          "or oracle.host/oracle.port/(oracle.serviceName|oracle.sid), plus oracle.user/oracle.password")
    }
  }

  test("OCI smoke SELECT 1 FROM DUAL") {
    val cfg = requireConfig()
    val conn = OciConnection.connect(cfg.connectString, cfg.user, cfg.password)
    var stmt: OciStatement = null
    var result: OciResultReader = null

    try {
      stmt = conn.prepareStatement("SELECT 1 AS X FROM DUAL")
      result = stmt.executeQuery()
      assert(result.hasNext, "Expected one row from DUAL")
      val row = result.next()
      assert(row.getInt(0) == 1)
    } finally {
      closeQuietly(result)
      closeQuietly(stmt)
      closeQuietly(conn)
    }
  }

  test("OCI query test on real table") {
    val cfg = requireConfig()
    val query = sys.props.get("oracle.it.testQuery").map(_.trim).filter(_.nonEmpty).getOrElse(defaultQuery)

    val conn = OciConnection.connect(cfg.connectString, cfg.user, cfg.password)
    var stmt: OciStatement = null
    var result: OciResultReader = null

    try {
      stmt = conn.prepareStatement(query)
      result = stmt.executeQuery()

      val schemaDebug = result.columns
        .map(c => s"${c.name}(ociType=${c.ociType},precision=${c.precision},scale=${c.scale},nullable=${c.nullable})")
        .mkString(", ")
      info(s"OCI query schema: $schemaDebug")

      var count = 0
      while (result.hasNext) {
        result.next()
        count += 1
      }
      info(s"OCI query row count: $count")
      assert(count > 0, s"Expected at least one row for query: $query")
    } finally {
      closeQuietly(result)
      closeQuietly(stmt)
      closeQuietly(conn)
    }
  }

  private def closeQuietly(resource: AutoCloseable): Unit = {
    if (resource != null) {
      try {
        resource.close()
      } catch {
        case _: Throwable =>
      }
    }
  }
}
