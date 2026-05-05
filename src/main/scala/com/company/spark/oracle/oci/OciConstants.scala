package com.company.spark.oracle.oci

object OciHandleType {
  val OCI_HTYPE_ENV = 1
  val OCI_HTYPE_ERROR = 2
  val OCI_HTYPE_SVCCTX = 3
  val OCI_HTYPE_STMT = 4
  val OCI_HTYPE_BIND = 5
  val OCI_HTYPE_DEFINE = 6
  val OCI_HTYPE_DESCRIBE = 7
  val OCI_HTYPE_SERVER = 8
  val OCI_HTYPE_SESSION = 9
  val OCI_HTYPE_TRANS = 10
  val OCI_HTYPE_COMPLEXOBJECT = 11
  val OCI_HTYPE_SECURITY = 12
  val OCI_HTYPE_SUBSCRIPTION = 13
  val OCI_HTYPE_DIRPATH_CTX = 14
  val OCI_HTYPE_DIRPATH_COLUMN_ARRAY = 15
  val OCI_HTYPE_DIRPATH_STREAM = 16
  val OCI_HTYPE_PROC = 17
  val OCI_HTYPE_ADMIN = 19
  val OCI_HTYPE_EVENT = 20
}

object OciDescriptorType {
  val OCI_DTYPE_PARAM = 53
}

object OciReturnCode {
  val OCI_SUCCESS = 0
  val OCI_SUCCESS_WITH_INFO = 1
  val OCI_NEED_DATA = 99
  val OCI_NO_DATA = 100
  val OCI_ERROR = -1
  val OCI_INVALID_HANDLE = -2
  val OCI_STILL_EXECUTING = -3123
  val OCI_CONTINUE = -24200
}

object OciMode {
  val OCI_DEFAULT = 0x00000000
  val OCI_THREADED = 0x00000001
  val OCI_OBJECT = 0x00000002
  val OCI_EVENTS = 0x00000004
  val OCI_SHARED = 0x00000010
  val OCI_NO_UCB = 0x00000040
  val OCI_NO_MUTEX = 0x00000080
  val OCI_NEW_LENGTH_SEMANTICS = 0x00020000
  val OCI_DESCRIBE_ONLY = 0x00000010
  val OCI_COMMIT_ON_SUCCESS = 0x00000020
  val OCI_EXACT_FETCH = 0x00000002
  val OCI_STMT_SCROLLABLE_READONLY = 0x00000008
}

object OciAttr {
  val OCI_ATTR_SERVER = 6
  val OCI_ATTR_SESSION = 7
  val OCI_ATTR_TRANS = 8
  val OCI_ATTR_ROW_COUNT = 9
  val OCI_ATTR_PREFETCH_ROWS = 11
  val OCI_ATTR_PREFETCH_MEMORY = 13
  val OCI_ATTR_USERNAME = 22
  val OCI_ATTR_PASSWORD = 23
  val OCI_ATTR_STMT_TYPE = 24
  val OCI_ATTR_PARAM_COUNT = 18
  val OCI_ATTR_DATA_SIZE = 1
  val OCI_ATTR_DATA_TYPE = 2
  val OCI_ATTR_NAME = 4
  val OCI_ATTR_PRECISION = 5
  val OCI_ATTR_SCALE = 6
  val OCI_ATTR_IS_NULL = 7
  val OCI_ATTR_TYPE_NAME = 8
  val OCI_ATTR_SCHEMA_NAME = 9
  val OCI_ATTR_CHARSET_ID = 31
  val OCI_ATTR_CHARSET_FORM = 32
  val OCI_ATTR_ROWS_FETCHED = 197
}

object OciCredential {
  val OCI_CRED_RDBMS = 1
  val OCI_CRED_EXT = 2
  val OCI_CRED_PROXY = 3
}

object OciDataType {
  val SQLT_CHR = 1       // VARCHAR2
  val SQLT_NUM = 2       // NUMBER
  val SQLT_INT = 3       // INTEGER (machine native)
  val SQLT_FLT = 4       // FLOAT (machine native)
  val SQLT_STR = 5       // NULL-terminated string
  val SQLT_VNU = 6       // VARNUM
  val SQLT_PDN = 7       // packed decimal
  val SQLT_LNG = 8       // LONG
  val SQLT_VCS = 9       // VARCHAR
  val SQLT_NON = 10      // Null/empty PCC Descriptor entry
  val SQLT_RID = 11      // ROWID (deprecated)
  val SQLT_DAT = 12      // DATE
  val SQLT_VBI = 15      // VARRAW
  val SQLT_BFLOAT = 21   // BINARY_FLOAT
  val SQLT_BDOUBLE = 22  // BINARY_DOUBLE
  val SQLT_BIN = 23      // RAW
  val SQLT_LBI = 24      // LONG RAW
  val SQLT_UIN = 68      // unsigned int
  val SQLT_SLS = 91      // Display sign leading separate
  val SQLT_LVC = 94      // Long VARCHAR
  val SQLT_LVB = 95      // Long VARRAW
  val SQLT_AFC = 96      // CHAR (fixed)
  val SQLT_AVC = 97      // CHARZ
  val SQLT_IBFLOAT = 100  // Binary float canonical
  val SQLT_IBDOUBLE = 101 // Binary double canonical
  val SQLT_RDD = 104      // ROWID descriptor
  val SQLT_LAB = 105      // MLSLABEL
  val SQLT_NTY = 108      // Named type (object)
  val SQLT_REF = 110      // REF
  val SQLT_CLOB = 112     // CLOB
  val SQLT_BLOB = 113     // BLOB
  val SQLT_BFILE = 114    // BFILE
  val SQLT_CFILE = 115    // CFILE
  val SQLT_RSET = 116     // Result set
  val SQLT_NCO = 122      // Named collection
  val SQLT_VST = 155      // OCIString
  val SQLT_ODT = 156      // OCIDate
  val SQLT_TIMESTAMP = 187        // TIMESTAMP
  val SQLT_TIMESTAMP_TZ = 188     // TIMESTAMP WITH TIME ZONE
  val SQLT_INTERVAL_YM = 189      // INTERVAL YEAR TO MONTH
  val SQLT_INTERVAL_DS = 190      // INTERVAL DAY TO SECOND
  val SQLT_TIMESTAMP_LTZ = 232    // TIMESTAMP WITH LOCAL TIME ZONE
  val SQLT_PNTY = 241             // PL/SQL named type
  val SQLT_REC = 250              // PL/SQL record
  val SQLT_TAB = 251              // PL/SQL table
  val SQLT_BOL = 252              // PL/SQL boolean
}

object OciFetchOrientation {
  val OCI_FETCH_CURRENT: Short = 0x00000001
  val OCI_FETCH_NEXT: Short = 0x00000002
  val OCI_FETCH_FIRST: Short = 0x00000004
  val OCI_FETCH_LAST: Short = 0x00000008
  val OCI_FETCH_PRIOR: Short = 0x00000010
  val OCI_FETCH_ABSOLUTE: Short = 0x00000020
  val OCI_FETCH_RELATIVE: Short = 0x00000040
}

object OciLanguage {
  val OCI_NTV_SYNTAX = 1
  val OCI_V7_SYNTAX = 2
  val OCI_V8_SYNTAX = 3
}

object OciCharsetForm {
  val SQLCS_IMPLICIT: Byte = 1
  val SQLCS_NCHAR: Byte = 2
  val SQLCS_EXPLICIT: Byte = 3
  val SQLCS_FLEXIBLE: Byte = 4
  val SQLCS_LIT_NULL: Byte = 5
}
