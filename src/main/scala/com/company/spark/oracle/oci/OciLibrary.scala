package com.company.spark.oracle.oci

import com.sun.jna.{Library, Native, NativeLong, Platform, Pointer}
import com.sun.jna.ptr.{IntByReference, LongByReference, PointerByReference}

trait OciLibrary extends Library {

  // Environment
  def OCIEnvNlsCreate(
      envhpp: PointerByReference,
      mode: Int,
      ctxp: Pointer,
      malocfp: Pointer,
      ralocfp: Pointer,
      mfreefp: Pointer,
      xtramemsz: NativeLong,
      usrmempp: Pointer,
      charset: Short,
      ncharset: Short): Int

  // Handle allocation/free
  def OCIHandleAlloc(
      parenth: Pointer,
      hndlpp: PointerByReference,
      htype: Int,
      xtramem_sz: NativeLong,
      usrmempp: Pointer): Int

  def OCIHandleFree(hndlp: Pointer, htype: Int): Int

  // Attribute get/set
  def OCIAttrGet(
      trgthndlp: Pointer,
      trghndltyp: Int,
      attributep: Pointer,
      sizep: IntByReference,
      attrtype: Int,
      errhp: Pointer): Int

  def OCIAttrSet(
      trgthndlp: Pointer,
      trghndltyp: Int,
      attributep: Pointer,
      size: Int,
      attrtype: Int,
      errhp: Pointer): Int

  // Server connection
  def OCIServerAttach(
      srvhp: Pointer,
      errhp: Pointer,
      dblink: Array[Byte],
      dblink_len: Int,
      mode: Int): Int

  def OCIServerDetach(srvhp: Pointer, errhp: Pointer, mode: Int): Int

  // Session
  def OCISessionBegin(
      svchp: Pointer,
      errhp: Pointer,
      usrhp: Pointer,
      credt: Int,
      mode: Int): Int

  def OCISessionEnd(svchp: Pointer, errhp: Pointer, usrhp: Pointer, mode: Int): Int

  // Statement
  def OCIStmtPrepare2(
      svchp: Pointer,
      stmthpp: PointerByReference,
      errhp: Pointer,
      stmttext: Array[Byte],
      stmt_len: Int,
      key: Pointer,
      keylen: Int,
      language: Int,
      mode: Int): Int

  def OCIStmtRelease(
      stmtp: Pointer,
      errhp: Pointer,
      key: Pointer,
      keylen: Int,
      mode: Int): Int

  def OCIStmtExecute(
      svchp: Pointer,
      stmtp: Pointer,
      errhp: Pointer,
      iters: Int,
      rowoff: Int,
      snap_in: Pointer,
      snap_out: Pointer,
      mode: Int): Int

  def OCIStmtFetch2(
      stmtp: Pointer,
      errhp: Pointer,
      nrows: Int,
      orientation: Short,
      scrollOffset: Int,
      mode: Int): Int

  // Define (output columns)
  def OCIDefineByPos(
      stmtp: Pointer,
      defnpp: PointerByReference,
      errhp: Pointer,
      position: Int,
      valuep: Pointer,
      value_sz: Int,
      dty: Short,
      indp: Pointer,
      rlenp: Pointer,
      rcodep: Pointer,
      mode: Int): Int

  // Bind (input parameters)
  def OCIBindByPos(
      stmtp: Pointer,
      bindpp: PointerByReference,
      errhp: Pointer,
      position: Int,
      valuep: Pointer,
      value_sz: Int,
      dty: Short,
      indp: Pointer,
      alenp: Pointer,
      rcodep: Pointer,
      maxarr_len: Int,
      curelep: Pointer,
      mode: Int): Int

  // Error handling
  def OCIErrorGet(
      hndlp: Pointer,
      recordno: Int,
      sqlstate: Pointer,
      errcodep: IntByReference,
      bufp: Pointer,
      bufsiz: Int,
      htype: Int): Int

  // Parameter (for metadata)
  def OCIParamGet(
      hndlp: Pointer,
      htype: Int,
      errhp: Pointer,
      parmdpp: PointerByReference,
      pos: Int): Int

  // Describe
  def OCIDescribeAny(
      svchp: Pointer,
      errhp: Pointer,
      objptr: Pointer,
      objnm_len: Int,
      objptr_typ: Byte,
      info_level: Byte,
      objtyp: Byte,
      dschp: Pointer): Int

  // LOB operations
  def OCILobRead2(
      svchp: Pointer,
      errhp: Pointer,
      locp: Pointer,
      byte_amtp: LongByReference,
      char_amtp: LongByReference,
      offset: Long,
      bufp: Pointer,
      bufl: Long,
      piece: Byte,
      ctxp: Pointer,
      cbfp: Pointer,
      csid: Short,
      csfrm: Byte): Int

  // Number conversion
  def OCINumberToReal(
      err: Pointer,
      number: Pointer,
      rsl_length: Int,
      rsl: Pointer): Int

  def OCINumberToInt(
      err: Pointer,
      number: Pointer,
      rsl_length: Int,
      rsl_flag: Int,
      rsl: Pointer): Int

  // Date conversion
  def OCIDateToText(
      err: Pointer,
      date: Pointer,
      fmt: Pointer,
      fmt_length: Byte,
      lang_name: Pointer,
      lang_length: Int,
      buf_size: IntByReference,
      buf: Pointer): Int
}

object OciLibrary {
  private val libraryName: String = {
    if (Platform.isWindows) "oci"
    else if (Platform.isMac) "clntsh"
    else "clntsh"
  }

  private var _instance: OciLibrary = _
  private val lock = new Object

  def instance: OciLibrary = {
    if (_instance == null) {
      lock.synchronized {
        if (_instance == null) {
          try {
            _instance = Native.load(libraryName, classOf[OciLibrary])
          } catch {
            case e: UnsatisfiedLinkError =>
              throw new OciException(
                OciErrorInfo(-1, s"Failed to load Oracle OCI library '$libraryName'. " +
                  "Ensure Oracle Instant Client is installed and library path is configured. " +
                  s"Error: ${e.getMessage}", None),
                e)
          }
        }
      }
    }
    _instance
  }

  def isAvailable: Boolean = {
    try {
      instance
      true
    } catch {
      case _: OciException => false
    }
  }
}
