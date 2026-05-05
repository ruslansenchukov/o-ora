package com.company.spark.oracle.oci

import com.sun.jna.{Memory, Pointer}
import com.sun.jna.ptr.{IntByReference, PointerByReference}

import java.nio.charset.StandardCharsets
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer

final class OciResultReader private[oci] (
    private val stmtHandle: Pointer,
    private val errorHandle: Pointer,
    val columns: Seq[OciColumnDef],
    private val _prefetchSize: Int,
    private val owningStatement: OciStatement)
    extends Iterator[OciRow] with AutoCloseable {

  private val fetchBatchSize = math.max(1, _prefetchSize)
  private val defines = columns.map(col => OciDefine.create(stmtHandle, errorHandle, col, fetchBatchSize))
  private var prefetchedRows: ArrayBuffer[OciRow] = ArrayBuffer.empty
  private var rowIndex = 0
  private var exhausted = false
  @volatile private var closed = false

  override def hasNext: Boolean = {
    if (closed) return false
    if (rowIndex < prefetchedRows.size) return true
    if (exhausted) return false

    fetchNextBatch()

    rowIndex < prefetchedRows.size
  }

  override def next(): OciRow = {
    if (!hasNext) throw new NoSuchElementException("No more rows")
    val row = prefetchedRows(rowIndex)
    rowIndex += 1
    row
  }

  private def fetchNextBatch(): Unit = {
    prefetchedRows.clear()
    rowIndex = 0

    val status = OciLibrary.instance.OCIStmtFetch2(
      stmtHandle,
      errorHandle,
      fetchBatchSize,
      OciFetchOrientation.OCI_FETCH_NEXT,
      0,
      OciMode.OCI_DEFAULT)

    status match {
      case OciReturnCode.OCI_SUCCESS | OciReturnCode.OCI_SUCCESS_WITH_INFO =>
        extractRows()

      case OciReturnCode.OCI_NO_DATA =>
        exhausted = true
        extractRows() // May have partial results

      case _ =>
        OciError.checkStatus(status, errorHandle, "Failed to fetch rows")
    }
  }

  private def extractRows(): Unit = {
    val rowsFetched = getRowsFetched
    prefetchedRows = new ArrayBuffer[OciRow](rowsFetched)
    var row = 0
    while (row < rowsFetched) {
      val values = new Array[Any](defines.size)
      var i = 0
      while (i < defines.size) {
        values(i) = defines(i).getValue
        i += 1
      }
      prefetchedRows += OciRow(values)
      // Move define buffers to next row position
      defines.foreach(_.advanceRow())
      row += 1
    }
    // Reset defines for next batch
    defines.foreach(_.resetRow())
  }

  private def getRowsFetched: Int = {
    val value = new Memory(4)
    val status = OciLibrary.instance.OCIAttrGet(
      stmtHandle,
      OciHandleType.OCI_HTYPE_STMT,
      value,
      null,
      OciAttr.OCI_ATTR_ROWS_FETCHED,
      errorHandle)

    if (OciError.isSuccess(status)) value.getInt(0) else 0
  }

  override def close(): Unit = synchronized {
    if (!closed) {
      closed = true
      defines.foreach(_.close())
      prefetchedRows.clear()
      owningStatement.close()
    }
  }
}

final case class OciRow(values: Array[Any]) {
  def get(index: Int): Any = values(index)
  def getString(index: Int): String = Option(values(index)).map(_.toString).orNull
  def getLong(index: Int): java.lang.Long = values(index) match {
    case null => null
    case n: Number => n.longValue()
    case s: String => java.lang.Long.parseLong(s)
    case other => java.lang.Long.parseLong(other.toString)
  }
  def getInt(index: Int): java.lang.Integer = values(index) match {
    case null => null
    case n: Number => n.intValue()
    case s: String => java.lang.Integer.parseInt(s)
    case other => java.lang.Integer.parseInt(other.toString)
  }
  def getDouble(index: Int): java.lang.Double = values(index) match {
    case null => null
    case n: Number => n.doubleValue()
    case s: String => java.lang.Double.parseDouble(s)
    case other => java.lang.Double.parseDouble(other.toString)
  }
  def getBigDecimal(index: Int): java.math.BigDecimal = values(index) match {
    case null => null
    case bd: java.math.BigDecimal => bd
    case bd: scala.math.BigDecimal => bd.bigDecimal
    case n: Number => new java.math.BigDecimal(n.toString)
    case s: String => new java.math.BigDecimal(s)
    case other => new java.math.BigDecimal(other.toString)
  }
  def getTimestamp(index: Int): java.sql.Timestamp = values(index) match {
    case null => null
    case ts: java.sql.Timestamp => ts
    case d: java.sql.Date => new java.sql.Timestamp(d.getTime)
    case other => java.sql.Timestamp.valueOf(other.toString)
  }
  def getDate(index: Int): java.sql.Date = values(index) match {
    case null => null
    case d: java.sql.Date => d
    case ts: java.sql.Timestamp => new java.sql.Date(ts.getTime)
    case other => java.sql.Date.valueOf(other.toString.take(10))
  }
  def getBytes(index: Int): Array[Byte] = values(index) match {
    case null => null
    case b: Array[Byte] => b
    case other => other.toString.getBytes(StandardCharsets.UTF_8)
  }
}

final class OciDefine private (
    private val valueMemory: Memory,
    private val indicatorMemory: Memory,
    private val lengthMemory: Memory,
    private val column: OciColumnDef,
    private var currentRowOffset: Int)
    extends AutoCloseable {

  private val rowSize = column.dataSize

  def getValue: Any = {
    val indicator = indicatorMemory.getShort(currentRowOffset * 2)
    if (indicator == -1) {
      return null
    }

    val offset = currentRowOffset * rowSize
    val rawLength =
      if (lengthMemory != null) lengthMemory.getShort(currentRowOffset * 2).toInt & 0xFFFF
      else column.dataSize
    val remaining = math.max(0, valueMemory.size() - offset.toLong).toInt
    val actualLength = math.min(rawLength, remaining)

    column.ociType match {
      case OciDataType.SQLT_CHR | OciDataType.SQLT_STR | OciDataType.SQLT_AFC |
           OciDataType.SQLT_AVC | OciDataType.SQLT_VCS | OciDataType.SQLT_LNG |
           OciDataType.SQLT_RDD =>
        val bytes = valueMemory.getByteArray(offset, math.min(actualLength, column.dataSize))
        new String(bytes, StandardCharsets.UTF_8).trim

      case OciDataType.SQLT_NUM | OciDataType.SQLT_VNU =>
        // NUMBER returned as string for precision
        val bytes = valueMemory.getByteArray(offset, math.min(actualLength, column.dataSize))
        val str = new String(bytes, StandardCharsets.UTF_8).trim
        if (str.isEmpty) null
        else {
          try {
            if (column.scale == 0 && column.precision > 0 && column.precision <= 9) {
              java.lang.Integer.valueOf(str)
            } else if (column.scale == 0 && column.precision > 0 && column.precision <= 18) {
              java.lang.Long.valueOf(str)
            } else {
              new java.math.BigDecimal(str)
            }
          } catch {
            case _: NumberFormatException => str
          }
        }

      case OciDataType.SQLT_INT =>
        valueMemory.getLong(offset)

      case OciDataType.SQLT_FLT | OciDataType.SQLT_BDOUBLE | OciDataType.SQLT_IBDOUBLE =>
        valueMemory.getDouble(offset)

      case OciDataType.SQLT_BFLOAT | OciDataType.SQLT_IBFLOAT =>
        valueMemory.getFloat(offset)

      case OciDataType.SQLT_DAT =>
        // Oracle DATE: 7 bytes
        val century = (valueMemory.getByte(offset) & 0xFF) - 100
        val year = (valueMemory.getByte(offset + 1) & 0xFF) - 100
        val month = valueMemory.getByte(offset + 2) & 0xFF
        val day = valueMemory.getByte(offset + 3) & 0xFF
        val hour = (valueMemory.getByte(offset + 4) & 0xFF) - 1
        val minute = (valueMemory.getByte(offset + 5) & 0xFF) - 1
        val second = (valueMemory.getByte(offset + 6) & 0xFF) - 1

        val cal = Calendar.getInstance()
        cal.set(century * 100 + year, month - 1, day, hour, minute, second)
        cal.set(Calendar.MILLISECOND, 0)
        new java.sql.Timestamp(cal.getTimeInMillis)

      case OciDataType.SQLT_TIMESTAMP | OciDataType.SQLT_TIMESTAMP_TZ | OciDataType.SQLT_TIMESTAMP_LTZ =>
        // Timestamp returned as string
        val bytes = valueMemory.getByteArray(offset, math.min(actualLength, column.dataSize))
        val str = new String(bytes, StandardCharsets.UTF_8).trim
        if (str.isEmpty) null
        else try {
          java.sql.Timestamp.valueOf(str)
        } catch {
          case _: IllegalArgumentException =>
            try {
              java.sql.Timestamp.valueOf(str.replace('T', ' ').take(23))
            } catch {
              case _: IllegalArgumentException => str
            }
        }

      case OciDataType.SQLT_BIN | OciDataType.SQLT_LBI | OciDataType.SQLT_BLOB =>
        valueMemory.getByteArray(offset, actualLength)

      case OciDataType.SQLT_CLOB =>
        val bytes = valueMemory.getByteArray(offset, actualLength)
        new String(bytes, StandardCharsets.UTF_8)

      case _ =>
        // Default: return as string
        val bytes = valueMemory.getByteArray(offset, math.min(actualLength, column.dataSize))
        new String(bytes, StandardCharsets.UTF_8).trim
    }
  }

  def advanceRow(): Unit = {
    currentRowOffset += 1
  }

  def resetRow(): Unit = {
    currentRowOffset = 0
  }

  override def close(): Unit = {
    // Memory will be garbage collected
  }
}

object OciDefine {
  private val DefaultStringBufferSize = 1024

  def create(stmtHandle: Pointer, errorHandle: Pointer, column: OciColumnDef, fetchRows: Int): OciDefine = {
    val effectiveFetchRows = math.max(1, fetchRows)
    val (bufferSize, ociType) = getBufferConfig(column)

    val valueMem = new Memory(bufferSize.toLong * effectiveFetchRows.toLong)
    val indMem = new Memory(2L * effectiveFetchRows.toLong)
    val lenMem = new Memory(2L * effectiveFetchRows.toLong)

    val defineRef = new PointerByReference()
    val status = OciLibrary.instance.OCIDefineByPos(
      stmtHandle,
      defineRef,
      errorHandle,
      column.position,
      valueMem,
      bufferSize,
      ociType,
      indMem,
      lenMem,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to define column ${column.name}")

    new OciDefine(valueMem, indMem, lenMem, column.copy(dataSize = bufferSize), 0)
  }

  private def getBufferConfig(column: OciColumnDef): (Int, Short) = {
    column.ociType match {
      case OciDataType.SQLT_CHR | OciDataType.SQLT_AFC | OciDataType.SQLT_AVC |
           OciDataType.SQLT_VCS | OciDataType.SQLT_LNG | OciDataType.SQLT_RDD =>
        // VARCHAR2, CHAR, LONG, ROWID -> String
        val estimated = if (column.dataSize > 0) column.dataSize * 4 else DefaultStringBufferSize
        val size = math.max(estimated, DefaultStringBufferSize) // UTF-8 expansion
        (size, OciDataType.SQLT_STR.toShort)

      case OciDataType.SQLT_NUM | OciDataType.SQLT_VNU =>
        // NUMBER -> String for precision
        (50, OciDataType.SQLT_STR.toShort)

      case OciDataType.SQLT_INT =>
        (8, OciDataType.SQLT_INT.toShort)

      case OciDataType.SQLT_FLT | OciDataType.SQLT_BDOUBLE | OciDataType.SQLT_IBDOUBLE =>
        (8, OciDataType.SQLT_BDOUBLE.toShort)

      case OciDataType.SQLT_BFLOAT | OciDataType.SQLT_IBFLOAT =>
        (4, OciDataType.SQLT_BFLOAT.toShort)

      case OciDataType.SQLT_DAT =>
        (7, OciDataType.SQLT_DAT.toShort)

      case OciDataType.SQLT_TIMESTAMP | OciDataType.SQLT_TIMESTAMP_TZ | OciDataType.SQLT_TIMESTAMP_LTZ =>
        // TIMESTAMP -> String
        (50, OciDataType.SQLT_STR.toShort)

      case OciDataType.SQLT_BIN | OciDataType.SQLT_LBI =>
        // RAW, LONG RAW
        val size = if (column.dataSize > 0) column.dataSize else DefaultStringBufferSize
        (size, OciDataType.SQLT_BIN.toShort)

      case OciDataType.SQLT_CLOB =>
        // CLOB -> limited string
        (32000, OciDataType.SQLT_STR.toShort)

      case OciDataType.SQLT_BLOB =>
        // BLOB -> limited binary
        (32000, OciDataType.SQLT_BIN.toShort)

      case _ =>
        // Default: string
        val estimated = if (column.dataSize > 0) column.dataSize * 4 else DefaultStringBufferSize
        val size = math.max(estimated, DefaultStringBufferSize)
        (size, OciDataType.SQLT_STR.toShort)
    }
  }
}
