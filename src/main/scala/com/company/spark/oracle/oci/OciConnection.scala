package com.company.spark.oracle.oci

import com.sun.jna.{Memory, NativeLong, Pointer}
import com.sun.jna.ptr.{IntByReference, PointerByReference}

import java.nio.charset.StandardCharsets

final class OciConnection private (
    private val envHandle: Pointer,
    private val errorHandle: Pointer,
    private val serverHandle: Pointer,
    private val sessionHandle: Pointer,
    private val serviceContextHandle: Pointer,
    val prefetchRows: Int)
    extends AutoCloseable {

  @volatile private var closed = false

  def prepareStatement(sql: String): OciStatement = {
    require(!closed, "Connection is closed")
    require(sql != null && sql.nonEmpty, "SQL cannot be null or empty")

    val stmtRef = new PointerByReference()
    val sqlBytes = sql.getBytes(StandardCharsets.UTF_8)

    val status = OciLibrary.instance.OCIStmtPrepare2(
      serviceContextHandle,
      stmtRef,
      errorHandle,
      sqlBytes,
      sqlBytes.length,
      Pointer.NULL,
      0,
      OciLanguage.OCI_NTV_SYNTAX,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to prepare statement")

    new OciStatement(stmtRef.getValue, errorHandle, serviceContextHandle, prefetchRows, this)
  }

  def isClosed: Boolean = closed

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true

      // Session end
      if (sessionHandle != null && sessionHandle != Pointer.NULL) {
        OciLibrary.instance.OCISessionEnd(
          serviceContextHandle,
          errorHandle,
          sessionHandle,
          OciMode.OCI_DEFAULT)
      }

      // Server detach
      if (serverHandle != null && serverHandle != Pointer.NULL) {
        OciLibrary.instance.OCIServerDetach(serverHandle, errorHandle, OciMode.OCI_DEFAULT)
      }

      // Free handles in reverse order
      freeHandle(serviceContextHandle, OciHandleType.OCI_HTYPE_SVCCTX)
      freeHandle(sessionHandle, OciHandleType.OCI_HTYPE_SESSION)
      freeHandle(serverHandle, OciHandleType.OCI_HTYPE_SERVER)
      freeHandle(errorHandle, OciHandleType.OCI_HTYPE_ERROR)
      freeHandle(envHandle, OciHandleType.OCI_HTYPE_ENV)
    }
  }

  private def freeHandle(handle: Pointer, htype: Int): Unit = {
    if (handle != null && handle != Pointer.NULL) {
      try {
        OciLibrary.instance.OCIHandleFree(handle, htype)
      } catch {
        case _: Throwable => // Ignore cleanup errors
      }
    }
  }
}

object OciConnection {

  def connect(
      connectString: String,
      user: String,
      password: String,
      prefetchRows: Int = 100): OciConnection = {

    require(connectString != null && connectString.nonEmpty, "Connect string cannot be empty")
    require(user != null && user.nonEmpty, "User cannot be empty")
    require(password != null, "Password cannot be null")

    var envHandle: Pointer = null
    var errorHandle: Pointer = null
    var serverHandle: Pointer = null
    var sessionHandle: Pointer = null
    var serviceContextHandle: Pointer = null

    try {
      // 1. Create environment
      val envRef = new PointerByReference()
      var status = OciLibrary.instance.OCIEnvNlsCreate(
        envRef,
        OciMode.OCI_THREADED | OciMode.OCI_OBJECT,
        Pointer.NULL,
        Pointer.NULL,
        Pointer.NULL,
        Pointer.NULL,
        new NativeLong(0),
        Pointer.NULL,
        0.toShort,
        0.toShort)

      if (status != OciReturnCode.OCI_SUCCESS) {
        throw new OciException(-1, s"OCIEnvNlsCreate failed with status $status")
      }
      envHandle = envRef.getValue

      // 2. Allocate error handle
      val errRef = new PointerByReference()
      status = OciLibrary.instance.OCIHandleAlloc(
        envHandle,
        errRef,
        OciHandleType.OCI_HTYPE_ERROR,
        new NativeLong(0),
        Pointer.NULL)

      if (status != OciReturnCode.OCI_SUCCESS) {
        throw new OciException(-1, s"Failed to allocate error handle: status $status")
      }
      errorHandle = errRef.getValue

      // 3. Allocate server handle
      val srvRef = new PointerByReference()
      status = OciLibrary.instance.OCIHandleAlloc(
        envHandle,
        srvRef,
        OciHandleType.OCI_HTYPE_SERVER,
        new NativeLong(0),
        Pointer.NULL)
      OciError.checkStatus(status, errorHandle, "Failed to allocate server handle")
      serverHandle = srvRef.getValue

      // 4. Attach to server
      val connectBytes = connectString.getBytes(StandardCharsets.UTF_8)
      status = OciLibrary.instance.OCIServerAttach(
        serverHandle,
        errorHandle,
        connectBytes,
        connectBytes.length,
        OciMode.OCI_DEFAULT)
      OciError.checkStatus(status, errorHandle, "Failed to attach to server")

      // 5. Allocate service context
      val svcRef = new PointerByReference()
      status = OciLibrary.instance.OCIHandleAlloc(
        envHandle,
        svcRef,
        OciHandleType.OCI_HTYPE_SVCCTX,
        new NativeLong(0),
        Pointer.NULL)
      OciError.checkStatus(status, errorHandle, "Failed to allocate service context")
      serviceContextHandle = svcRef.getValue

      // 6. Set server on service context
      status = OciLibrary.instance.OCIAttrSet(
        serviceContextHandle,
        OciHandleType.OCI_HTYPE_SVCCTX,
        serverHandle,
        0,
        OciAttr.OCI_ATTR_SERVER,
        errorHandle)
      OciError.checkStatus(status, errorHandle, "Failed to set server attribute")

      // 7. Allocate session handle
      val sesRef = new PointerByReference()
      status = OciLibrary.instance.OCIHandleAlloc(
        envHandle,
        sesRef,
        OciHandleType.OCI_HTYPE_SESSION,
        new NativeLong(0),
        Pointer.NULL)
      OciError.checkStatus(status, errorHandle, "Failed to allocate session handle")
      sessionHandle = sesRef.getValue

      // 8. Set username
      val userBytes = user.getBytes(StandardCharsets.UTF_8)
      val userMem = new Memory(userBytes.length)
      userMem.write(0, userBytes, 0, userBytes.length)
      status = OciLibrary.instance.OCIAttrSet(
        sessionHandle,
        OciHandleType.OCI_HTYPE_SESSION,
        userMem,
        userBytes.length,
        OciAttr.OCI_ATTR_USERNAME,
        errorHandle)
      OciError.checkStatus(status, errorHandle, "Failed to set username")

      // 9. Set password
      val passBytes = password.getBytes(StandardCharsets.UTF_8)
      val passMem = new Memory(passBytes.length)
      passMem.write(0, passBytes, 0, passBytes.length)
      status = OciLibrary.instance.OCIAttrSet(
        sessionHandle,
        OciHandleType.OCI_HTYPE_SESSION,
        passMem,
        passBytes.length,
        OciAttr.OCI_ATTR_PASSWORD,
        errorHandle)
      OciError.checkStatus(status, errorHandle, "Failed to set password")

      // 10. Begin session
      status = OciLibrary.instance.OCISessionBegin(
        serviceContextHandle,
        errorHandle,
        sessionHandle,
        OciCredential.OCI_CRED_RDBMS,
        OciMode.OCI_DEFAULT)
      OciError.checkStatus(status, errorHandle, "Failed to begin session")

      // 11. Set session on service context
      status = OciLibrary.instance.OCIAttrSet(
        serviceContextHandle,
        OciHandleType.OCI_HTYPE_SVCCTX,
        sessionHandle,
        0,
        OciAttr.OCI_ATTR_SESSION,
        errorHandle)
      OciError.checkStatus(status, errorHandle, "Failed to set session attribute")

      new OciConnection(
        envHandle,
        errorHandle,
        serverHandle,
        sessionHandle,
        serviceContextHandle,
        prefetchRows)

    } catch {
      case e: Throwable =>
        // Cleanup on failure
        if (sessionHandle != null) {
          try {
            OciLibrary.instance.OCIHandleFree(sessionHandle, OciHandleType.OCI_HTYPE_SESSION)
          } catch { case _: Throwable => }
        }
        if (serviceContextHandle != null) {
          try {
            OciLibrary.instance.OCIHandleFree(serviceContextHandle, OciHandleType.OCI_HTYPE_SVCCTX)
          } catch { case _: Throwable => }
        }
        if (serverHandle != null) {
          try {
            OciLibrary.instance.OCIServerDetach(serverHandle, errorHandle, OciMode.OCI_DEFAULT)
            OciLibrary.instance.OCIHandleFree(serverHandle, OciHandleType.OCI_HTYPE_SERVER)
          } catch { case _: Throwable => }
        }
        if (errorHandle != null) {
          try {
            OciLibrary.instance.OCIHandleFree(errorHandle, OciHandleType.OCI_HTYPE_ERROR)
          } catch { case _: Throwable => }
        }
        if (envHandle != null) {
          try {
            OciLibrary.instance.OCIHandleFree(envHandle, OciHandleType.OCI_HTYPE_ENV)
          } catch { case _: Throwable => }
        }
        throw e
    }
  }

  def buildConnectString(host: String, port: Int, serviceName: Option[String], sid: Option[String]): String = {
    val serviceOrSid = serviceName
      .map(s => s"SERVICE_NAME=$s")
      .orElse(sid.map(s => s"SID=$s"))
      .getOrElse(throw new IllegalArgumentException("Either serviceName or sid must be provided"))

    s"(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=$host)(PORT=$port))(CONNECT_DATA=($serviceOrSid)))"
  }
}
