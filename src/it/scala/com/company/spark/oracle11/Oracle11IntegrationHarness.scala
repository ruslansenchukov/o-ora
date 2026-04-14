package com.company.spark.oracle11

import java.sql.DriverManager

import scala.util.Try
import scala.util.control.NonFatal

import org.testcontainers.containers.OracleContainer
import org.testcontainers.utility.DockerImageName

final case class Oracle11IntegrationTarget(
    url: String,
    user: String,
    password: String,
    close: () => Unit)

object Oracle11IntegrationHarness {

  def resolve(): Option[Oracle11IntegrationTarget] = {
    val enabled = sys.props.get("oracle11.it.enabled").exists(_.toBoolean)
    if (!enabled) {
      return None
    }

    val externalUrl = sys.env.get("ORA11_IT_URL")
    val externalUser = sys.env.get("ORA11_IT_USER")
    val externalPassword = sys.env.get("ORA11_IT_PASSWORD")

    if (externalUrl.isDefined && externalUser.isDefined && externalPassword.isDefined) {
      return Some(
        Oracle11IntegrationTarget(
          externalUrl.get,
          externalUser.get,
          externalPassword.get,
          () => ()
        ))
    }

    val image = DockerImageName
      .parse(sys.props.getOrElse("oracle11.it.image", "gvenzl/oracle-xe:21-slim"))
      .asCompatibleSubstituteFor("gvenzl/oracle-xe")

    val container = new OracleContainer(image)

    try {
      container.start()
      Some(
        Oracle11IntegrationTarget(
          container.getJdbcUrl,
          container.getUsername,
          container.getPassword,
          () => container.stop()
        ))
    } catch {
      case NonFatal(err) =>
        System.err.println(
          s"[oracle11-it] Failed to start Testcontainers Oracle XE: ${err.getMessage}. " +
            "Use ORA11_IT_URL/ORA11_IT_USER/ORA11_IT_PASSWORD for an external DB, or set " +
            "TESTCONTAINERS_RYUK_DISABLED=true when running on Colima.")
        Try(container.stop())
        None
    }
  }

  def initializeTable(target: Oracle11IntegrationTarget, tableName: String): Unit = {
    val conn = DriverManager.getConnection(target.url, target.user, target.password)
    var stmt: java.sql.Statement = null
    try {
      conn.setAutoCommit(false)
      stmt = conn.createStatement()
      Try(stmt.executeUpdate(s"DROP TABLE $tableName PURGE"))
      stmt.executeUpdate(
        s"CREATE TABLE $tableName (" +
          "ID NUMBER(9) NOT NULL, " +
          "NAME VARCHAR2(100), " +
          "SALARY NUMBER(12,2), " +
          "CREATED_AT TIMESTAMP, " +
          "PAYLOAD RAW(16)" +
          ")")

      stmt.executeUpdate(
        s"INSERT INTO $tableName (ID, NAME, SALARY, CREATED_AT, PAYLOAD) VALUES " +
          "(1, 'Alice', 1000.50, TIMESTAMP '2020-01-01 00:00:00', HEXTORAW('A1'))")
      stmt.executeUpdate(
        s"INSERT INTO $tableName (ID, NAME, SALARY, CREATED_AT, PAYLOAD) VALUES " +
          "(2, 'Bob', 2000.00, TIMESTAMP '2020-01-02 00:00:00', HEXTORAW('B2'))")
      stmt.executeUpdate(
        s"INSERT INTO $tableName (ID, NAME, SALARY, CREATED_AT, PAYLOAD) VALUES " +
          "(3, NULL, 3000.75, TIMESTAMP '2020-01-03 00:00:00', HEXTORAW('C3'))")
      conn.commit()
    } finally {
      Oracle11JdbcUtils.closeQuietly(stmt)
      Oracle11JdbcUtils.closeQuietly(conn)
    }
  }
}
