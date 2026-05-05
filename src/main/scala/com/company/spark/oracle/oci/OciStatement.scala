package com.company.spark.oracle.oci

import com.sun.jna.{Memory, Native, Pointer}
import com.sun.jna.ptr.{IntByReference, PointerByReference}

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

final class OciStatement private[oci] (
    private val stmtHandle: Pointer,
    private val errorHandle: Pointer,
    private val serviceContext: Pointer,
    private val prefetchRows: Int,
    private val connection: OciConnection)
    extends AutoCloseable {

  private val binds = new ArrayBuffer[OciBind]()
  @volatile private var closed = false

  def bindString(position: Int, value: String): Unit = {
    require(!closed, "Statement is closed")
    val bind = OciBind.createString(stmtHandle, errorHandle, position, value)
    binds += bind
  }

  def bindNumber(position: Int, value: BigDecimal): Unit = {
    require(!closed, "Statement is closed")
    val bind = OciBind.createNumber(stmtHandle, errorHandle, position, value)
    binds += bind
  }

  def bindLong(position: Int, value: Long): Unit = {
    require(!closed, "Statement is closed")
    val bind = OciBind.createLong(stmtHandle, errorHandle, position, value)
    binds += bind
  }

  def bindInt(position: Int, value: Int): Unit = {
    require(!closed, "Statement is closed")
    val bind = OciBind.createInt(stmtHandle, errorHandle, position, value)
    binds += bind
  }

  def bindDouble(position: Int, value: Double): Unit = {
    require(!closed, "Statement is closed")
    val bind = OciBind.createDouble(stmtHandle, errorHandle, position, value)
    binds += bind
  }

  def bindTimestamp(position: Int, value: java.sql.Timestamp): Unit = {
    require(!closed, "Statement is closed")
    val bind = OciBind.createTimestamp(stmtHandle, errorHandle, position, value)
    binds += bind
  }

  def bindDate(position: Int, value: java.sql.Date): Unit = {
    require(!closed, "Statement is closed")
    val bind = OciBind.createDate(stmtHandle, errorHandle, position, value)
    binds += bind
  }

  def executeQuery(): OciResultReader = {
    require(!closed, "Statement is closed")

    // Execute in describe-only mode first to get column info
    var status = OciLibrary.instance.OCIStmtExecute(
      serviceContext,
      stmtHandle,
      errorHandle,
      0,
      0,
      Pointer.NULL,
      Pointer.NULL,
      OciMode.OCI_DESCRIBE_ONLY)

    OciError.checkStatus(status, errorHandle, "Failed to describe statement")

    // Get column count
    val columnCount = getIntAttr(OciAttr.OCI_ATTR_PARAM_COUNT)

    // Build column definitions
    val columns = (1 to columnCount).map { pos =>
      val paramRef = new PointerByReference()
      status = OciLibrary.instance.OCIParamGet(
        stmtHandle,
        OciHandleType.OCI_HTYPE_STMT,
        errorHandle,
        paramRef,
        pos)
      OciError.checkStatus(status, errorHandle, s"Failed to get parameter $pos")

      OciColumnDef.fromParam(paramRef.getValue, errorHandle, pos)
    }

    // Set prefetch
    setPrefetch(prefetchRows)

    // Execute for real
    status = OciLibrary.instance.OCIStmtExecute(
      serviceContext,
      stmtHandle,
      errorHandle,
      0,
      0,
      Pointer.NULL,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, "Failed to execute statement")

    new OciResultReader(stmtHandle, errorHandle, columns, prefetchRows, this)
  }

  def execute(): Int = {
    require(!closed, "Statement is closed")

    val status = OciLibrary.instance.OCIStmtExecute(
      serviceContext,
      stmtHandle,
      errorHandle,
      1,
      0,
      Pointer.NULL,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, "Failed to execute statement")

    getIntAttr(OciAttr.OCI_ATTR_ROW_COUNT)
  }

  private def setPrefetch(rows: Int): Unit = {
    val mem = new Memory(4)
    mem.setInt(0, rows)

    val status = OciLibrary.instance.OCIAttrSet(
      stmtHandle,
      OciHandleType.OCI_HTYPE_STMT,
      mem,
      4,
      OciAttr.OCI_ATTR_PREFETCH_ROWS,
      errorHandle)

    // Ignore errors for prefetch setting
    if (!OciError.isSuccess(status)) {
      // Log warning but continue
    }
  }

  private def getIntAttr(attr: Int): Int = {
    val value = new Memory(4)
    val size = new IntByReference()

    val status = OciLibrary.instance.OCIAttrGet(
      stmtHandle,
      OciHandleType.OCI_HTYPE_STMT,
      value,
      size,
      attr,
      errorHandle)

    OciError.checkStatus(status, errorHandle, s"Failed to get attribute $attr")
    value.getInt(0)
  }

  def isClosed: Boolean = closed

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true

      // Close all binds
      binds.foreach(_.close())
      binds.clear()

      // Release statement
      if (stmtHandle != null && stmtHandle != Pointer.NULL) {
        try {
          OciLibrary.instance.OCIStmtRelease(
            stmtHandle,
            errorHandle,
            Pointer.NULL,
            0,
            OciMode.OCI_DEFAULT)
        } catch {
          case _: Throwable => // Ignore cleanup errors
        }
      }
    }
  }
}

final case class OciColumnDef(
    position: Int,
    name: String,
    ociType: Short,
    dataSize: Int,
    precision: Int,
    scale: Int,
    nullable: Boolean)

object OciColumnDef {
  private val MaxNameSize = 128

  def fromParam(paramHandle: Pointer, errorHandle: Pointer, position: Int): OciColumnDef = {
    // Get column name
    val nameBuf = new Memory(Native.POINTER_SIZE.toLong)
    val nameSize = new IntByReference()
    var status = OciLibrary.instance.OCIAttrGet(
      paramHandle,
      OciDescriptorType.OCI_DTYPE_PARAM,
      nameBuf,
      nameSize,
      OciAttr.OCI_ATTR_NAME,
      errorHandle)

    val name = if (OciError.isSuccess(status) && nameSize.getValue > 0) {
      val namePtr = nameBuf.getPointer(0)
      if (namePtr == null || namePtr == Pointer.NULL) {
        s"COL$position"
      } else {
        val bytes = namePtr.getByteArray(0, nameSize.getValue)
        new String(bytes, StandardCharsets.UTF_8).trim
      }
    } else {
      s"COL$position"
    }

    // Get data type
    val typeBuf = new Memory(2)
    status = OciLibrary.instance.OCIAttrGet(
      paramHandle,
      OciDescriptorType.OCI_DTYPE_PARAM,
      typeBuf,
      null,
      OciAttr.OCI_ATTR_DATA_TYPE,
      errorHandle)
    val ociType = if (OciError.isSuccess(status)) typeBuf.getShort(0) else OciDataType.SQLT_CHR.toShort

    // Get data size
    val sizeBuf = new Memory(4)
    status = OciLibrary.instance.OCIAttrGet(
      paramHandle,
      OciDescriptorType.OCI_DTYPE_PARAM,
      sizeBuf,
      null,
      OciAttr.OCI_ATTR_DATA_SIZE,
      errorHandle)
    val dataSize = if (OciError.isSuccess(status)) sizeBuf.getInt(0) else 4000

    // Get precision
    val precBuf = new Memory(2)
    status = OciLibrary.instance.OCIAttrGet(
      paramHandle,
      OciDescriptorType.OCI_DTYPE_PARAM,
      precBuf,
      null,
      OciAttr.OCI_ATTR_PRECISION,
      errorHandle)
    val precision = if (OciError.isSuccess(status)) precBuf.getShort(0).toInt else 0

    // Get scale
    val scaleBuf = new Memory(1)
    status = OciLibrary.instance.OCIAttrGet(
      paramHandle,
      OciDescriptorType.OCI_DTYPE_PARAM,
      scaleBuf,
      null,
      OciAttr.OCI_ATTR_SCALE,
      errorHandle)
    val scale = if (OciError.isSuccess(status)) scaleBuf.getByte(0).toInt else 0

    // Get nullable
    val nullBuf = new Memory(1)
    status = OciLibrary.instance.OCIAttrGet(
      paramHandle,
      OciDescriptorType.OCI_DTYPE_PARAM,
      nullBuf,
      null,
      OciAttr.OCI_ATTR_IS_NULL,
      errorHandle)
    val nullable = !OciError.isSuccess(status) || nullBuf.getByte(0) != 0

    OciColumnDef(position, name, ociType, dataSize, precision, scale, nullable)
  }
}
