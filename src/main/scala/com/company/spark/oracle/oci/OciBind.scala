package com.company.spark.oracle.oci

import com.sun.jna.{Memory, Pointer}
import com.sun.jna.ptr.PointerByReference

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.Calendar

final class OciBind private (
    private val valueMemory: Memory,
    private val indicatorMemory: Memory,
    private val bindHandle: Pointer)
    extends AutoCloseable {

  def close(): Unit = {
    // Memory will be garbage collected
    // OCI bind handles are freed when statement is released
  }
}

object OciBind {

  def createString(stmtHandle: Pointer, errorHandle: Pointer, position: Int, value: String): OciBind = {
    val bytes = if (value != null) value.getBytes(StandardCharsets.UTF_8) else Array.emptyByteArray
    val bufSize = math.max(bytes.length + 1, 1)

    val valueMem = new Memory(bufSize)
    val indMem = new Memory(2)

    if (value == null) {
      indMem.setShort(0, -1)
    } else {
      valueMem.write(0, bytes, 0, bytes.length)
      valueMem.setByte(bytes.length, 0)
      indMem.setShort(0, 0)
    }

    val bindRef = new PointerByReference()
    val status = OciLibrary.instance.OCIBindByPos(
      stmtHandle,
      bindRef,
      errorHandle,
      position,
      valueMem,
      bufSize,
      OciDataType.SQLT_STR.toShort,
      indMem,
      Pointer.NULL,
      Pointer.NULL,
      0,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to bind string at position $position")

    new OciBind(valueMem, indMem, bindRef.getValue)
  }

  def createNumber(stmtHandle: Pointer, errorHandle: Pointer, position: Int, value: BigDecimal): OciBind = {
    // Bind NUMBER as string for simplicity and precision
    val strValue = if (value != null) value.toString() else null
    createString(stmtHandle, errorHandle, position, strValue)
  }

  def createLong(stmtHandle: Pointer, errorHandle: Pointer, position: Int, value: Long): OciBind = {
    val valueMem = new Memory(8)
    val indMem = new Memory(2)

    valueMem.setLong(0, value)
    indMem.setShort(0, 0)

    val bindRef = new PointerByReference()
    val status = OciLibrary.instance.OCIBindByPos(
      stmtHandle,
      bindRef,
      errorHandle,
      position,
      valueMem,
      8,
      OciDataType.SQLT_INT.toShort,
      indMem,
      Pointer.NULL,
      Pointer.NULL,
      0,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to bind long at position $position")

    new OciBind(valueMem, indMem, bindRef.getValue)
  }

  def createInt(stmtHandle: Pointer, errorHandle: Pointer, position: Int, value: Int): OciBind = {
    val valueMem = new Memory(4)
    val indMem = new Memory(2)

    valueMem.setInt(0, value)
    indMem.setShort(0, 0)

    val bindRef = new PointerByReference()
    val status = OciLibrary.instance.OCIBindByPos(
      stmtHandle,
      bindRef,
      errorHandle,
      position,
      valueMem,
      4,
      OciDataType.SQLT_INT.toShort,
      indMem,
      Pointer.NULL,
      Pointer.NULL,
      0,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to bind int at position $position")

    new OciBind(valueMem, indMem, bindRef.getValue)
  }

  def createDouble(stmtHandle: Pointer, errorHandle: Pointer, position: Int, value: Double): OciBind = {
    val valueMem = new Memory(8)
    val indMem = new Memory(2)

    valueMem.setDouble(0, value)
    indMem.setShort(0, 0)

    val bindRef = new PointerByReference()
    val status = OciLibrary.instance.OCIBindByPos(
      stmtHandle,
      bindRef,
      errorHandle,
      position,
      valueMem,
      8,
      OciDataType.SQLT_BDOUBLE.toShort,
      indMem,
      Pointer.NULL,
      Pointer.NULL,
      0,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to bind double at position $position")

    new OciBind(valueMem, indMem, bindRef.getValue)
  }

  def createTimestamp(stmtHandle: Pointer, errorHandle: Pointer, position: Int, value: java.sql.Timestamp): OciBind = {
    // Bind as string in Oracle timestamp format
    val strValue = if (value != null) {
      val fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
      fmt.format(value)
    } else null

    val bytes = if (strValue != null) strValue.getBytes(StandardCharsets.UTF_8) else Array.emptyByteArray
    val bufSize = math.max(bytes.length + 1, 1)

    val valueMem = new Memory(bufSize)
    val indMem = new Memory(2)

    if (strValue == null) {
      indMem.setShort(0, -1)
    } else {
      valueMem.write(0, bytes, 0, bytes.length)
      valueMem.setByte(bytes.length, 0)
      indMem.setShort(0, 0)
    }

    val bindRef = new PointerByReference()
    val status = OciLibrary.instance.OCIBindByPos(
      stmtHandle,
      bindRef,
      errorHandle,
      position,
      valueMem,
      bufSize,
      OciDataType.SQLT_STR.toShort,
      indMem,
      Pointer.NULL,
      Pointer.NULL,
      0,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to bind timestamp at position $position")

    new OciBind(valueMem, indMem, bindRef.getValue)
  }

  def createDate(stmtHandle: Pointer, errorHandle: Pointer, position: Int, value: java.sql.Date): OciBind = {
    // Oracle DATE internal format: 7 bytes
    // byte[0] = century + 100
    // byte[1] = year of century + 100
    // byte[2] = month
    // byte[3] = day
    // byte[4] = hour + 1
    // byte[5] = minute + 1
    // byte[6] = second + 1

    val valueMem = new Memory(7)
    val indMem = new Memory(2)

    if (value == null) {
      indMem.setShort(0, -1)
    } else {
      val cal = Calendar.getInstance()
      cal.setTime(value)

      val year = cal.get(Calendar.YEAR)
      val century = (year / 100) + 100
      val yearInCentury = (year % 100) + 100

      valueMem.setByte(0, century.toByte)
      valueMem.setByte(1, yearInCentury.toByte)
      valueMem.setByte(2, (cal.get(Calendar.MONTH) + 1).toByte)
      valueMem.setByte(3, cal.get(Calendar.DAY_OF_MONTH).toByte)
      valueMem.setByte(4, 1.toByte) // hour + 1
      valueMem.setByte(5, 1.toByte) // minute + 1
      valueMem.setByte(6, 1.toByte) // second + 1

      indMem.setShort(0, 0)
    }

    val bindRef = new PointerByReference()
    val status = OciLibrary.instance.OCIBindByPos(
      stmtHandle,
      bindRef,
      errorHandle,
      position,
      valueMem,
      7,
      OciDataType.SQLT_DAT.toShort,
      indMem,
      Pointer.NULL,
      Pointer.NULL,
      0,
      Pointer.NULL,
      OciMode.OCI_DEFAULT)

    OciError.checkStatus(status, errorHandle, s"Failed to bind date at position $position")

    new OciBind(valueMem, indMem, bindRef.getValue)
  }
}
