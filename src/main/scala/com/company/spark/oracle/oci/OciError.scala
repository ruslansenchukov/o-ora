package com.company.spark.oracle.oci

import com.sun.jna.{Memory, Pointer}
import com.sun.jna.ptr.IntByReference

final case class OciErrorInfo(
    code: Int,
    message: String,
    sqlState: Option[String])

class OciException(val errorInfo: OciErrorInfo, cause: Throwable = null)
    extends RuntimeException(s"OCI-${errorInfo.code}: ${errorInfo.message}", cause) {

  def this(code: Int, message: String) = this(OciErrorInfo(code, message, None), null)
  def this(code: Int, message: String, cause: Throwable) = this(OciErrorInfo(code, message, None), cause)
}

object OciError {
  private val MaxErrorMsgSize = 3072

  def check(status: Int, errorHandle: Pointer): Unit = {
    status match {
      case OciReturnCode.OCI_SUCCESS | OciReturnCode.OCI_SUCCESS_WITH_INFO =>
      // OK
      case OciReturnCode.OCI_NO_DATA =>
      // End of data - not an error
      case OciReturnCode.OCI_INVALID_HANDLE =>
        throw new OciException(OciErrorInfo(-2, "Invalid OCI handle", None))
      case _ =>
        throw new OciException(getError(errorHandle))
    }
  }

  def checkStatus(status: Int, errorHandle: Pointer, context: String): Unit = {
    status match {
      case OciReturnCode.OCI_SUCCESS | OciReturnCode.OCI_SUCCESS_WITH_INFO =>
      case OciReturnCode.OCI_NO_DATA =>
      case _ =>
        val error = getError(errorHandle)
        throw new OciException(
          OciErrorInfo(error.code, s"$context: ${error.message}", error.sqlState))
    }
  }

  def isNoData(status: Int): Boolean = status == OciReturnCode.OCI_NO_DATA

  def isSuccess(status: Int): Boolean =
    status == OciReturnCode.OCI_SUCCESS || status == OciReturnCode.OCI_SUCCESS_WITH_INFO

  def getError(errorHandle: Pointer): OciErrorInfo = {
    if (errorHandle == null || errorHandle == Pointer.NULL) {
      return OciErrorInfo(-1, "Unknown error (null error handle)", None)
    }

    val errcode = new IntByReference()
    val errmsg = new Memory(MaxErrorMsgSize)

    val status = OciLibrary.instance.OCIErrorGet(
      errorHandle,
      1,
      Pointer.NULL,
      errcode,
      errmsg,
      MaxErrorMsgSize,
      OciHandleType.OCI_HTYPE_ERROR)

    if (status == OciReturnCode.OCI_SUCCESS) {
      val message = errmsg.getString(0).trim
      OciErrorInfo(errcode.getValue, message, None)
    } else {
      OciErrorInfo(-1, s"Unknown error (OCIErrorGet returned $status)", None)
    }
  }
}
