package org.deephacks.lmdbjni.jnr;

import jnr.ffi.LibraryLoader;
import jnr.ffi.Pointer;
import static jnr.ffi.Runtime.getRuntime;
import jnr.ffi.Struct;
import jnr.ffi.Struct.size_t;
import jnr.ffi.annotations.Direct;
import jnr.ffi.annotations.In;
import jnr.ffi.annotations.Out;
import jnr.ffi.byref.IntByReference;
import jnr.ffi.byref.PointerByReference;

/**
 * Lowest level methods to access LMDB library. Shouldn't use directly.
 * Should make package protected if we keep JNR.
 */
public class Library {

  public static final jnr.ffi.Runtime runtime;
  public static final Lmdb lib;
  
  static {
    lib = LibraryLoader.create(Lmdb.class).load("lmdb");
    runtime = getRuntime(lib);
  }
  
  public static final class MDB_val extends Struct {
    public final size_t size = new size_t();
    public final Pointer data = new Pointer();

    public MDB_val(jnr.ffi.Runtime runtime) {
      super(runtime);
    }
  }

  public static final class Env extends Struct {
   public Env(jnr.ffi.Runtime runtime) {
      super(runtime);
    }
  }

  public interface Lmdb {
    // TODO: Wonder if flags|cursorOp can be made into enum for type safety?
    // TODO: Is it legal to subclass Pointer to get some type safety? eg TxPointer?
    int mdb_env_create(PointerByReference envPtr);
    int mdb_env_set_maxreaders(@In Pointer env, int readers);
    int mdb_env_set_mapsize(@In Pointer env, long size);
    int mdb_env_set_maxdbs(@In Pointer env, int dbs);
    int mdb_env_open(@In Pointer env, @In String path, int flags, int mode);
    int mdb_txn_begin(@In Pointer env, @In Pointer parentTx, int flags, Pointer txPtr);
    int mdb_dbi_open(@In Pointer txn, @In String name, int flags, IntByReference dbiPtr);
    int mdb_put(@In Pointer txn, int dbi, @In MDB_val key, @In MDB_val val, int flags);
    int mdb_get(@In Pointer ptr, int dbi, MDB_val k, @Out MDB_val v);
    int mdb_txn_commit(@In Pointer ptr);
    int mdb_cursor_open(@In Pointer ptr, int dbi, PointerByReference cursorPtr);
    int mdb_cursor_get(@In Pointer cursor, @Direct MDB_val k, @Direct @Out MDB_val v, int cursorOp);
    int mdb_cursor_get(@In Pointer cursor, Pointer k, @Out Pointer v, int cursorOp);
  }

// these were shamelessly copied from LMDB.java, generated by JavaCPP
  
/** \defgroup	mdb_env	Environment Flags
 *	\{
 */
	/** mmap at a fixed address (experimental) */
public static final int MDB_FIXEDMAP =	0x01;
	/** no environment directory */
public static final int MDB_NOSUBDIR =	0x4000;
	/** don't fsync after commit */
public static final int MDB_NOSYNC =		0x10000;
	/** read only */
public static final int MDB_RDONLY =		0x20000;
	/** don't fsync metapage after commit */
public static final int MDB_NOMETASYNC =		0x40000;
	/** use writable mmap */
public static final int MDB_WRITEMAP =		0x80000;
	/** use asynchronous msync when #MDB_WRITEMAP is used */
public static final int MDB_MAPASYNC =		0x100000;
	/** tie reader locktable slots to #MDB_txn objects instead of to threads */
public static final int MDB_NOTLS =		0x200000;
	/** don't do any locking, caller must manage their own locks */
public static final int MDB_NOLOCK =		0x400000;
	/** don't do readahead (no effect on Windows) */
public static final int MDB_NORDAHEAD =	0x800000;
	/** don't initialize malloc'd memory before writing to datafile */
public static final int MDB_NOMEMINIT =	0x1000000;
/** \} */

/**	\defgroup	mdb_dbi_open	Database Flags
 *	\{
 */
	/** use reverse string keys */
public static final int MDB_REVERSEKEY =	0x02;
	/** use sorted duplicates */
public static final int MDB_DUPSORT =		0x04;
	/** numeric keys in native byte order: either unsigned int or size_t.
	 *  The keys must all be of the same size. */
public static final int MDB_INTEGERKEY =	0x08;
	/** with #MDB_DUPSORT, sorted dup items have fixed size */
public static final int MDB_DUPFIXED =	0x10;
	/** with #MDB_DUPSORT, dups are #MDB_INTEGERKEY-style integers */
public static final int MDB_INTEGERDUP =	0x20;
	/** with #MDB_DUPSORT, use reverse string dups */
public static final int MDB_REVERSEDUP =	0x40;
	/** create DB if not already existing */
public static final int MDB_CREATE =		0x40000;
/** \} */

/**	\defgroup mdb_put	Write Flags
 *	\{
 */
/** For put: Don't write if the key already exists. */
public static final int MDB_NOOVERWRITE =	0x10;
/** Only for #MDB_DUPSORT<br>
 * For put: don't write if the key and data pair already exist.<br>
 * For mdb_cursor_del: remove all duplicate data items.
 */
public static final int MDB_NODUPDATA =	0x20;
/** For mdb_cursor_put: overwrite the current key/data pair */
public static final int MDB_CURRENT =	0x40;
/** For put: Just reserve space for data, don't copy it. Return a
 * pointer to the reserved space.
 */
public static final int MDB_RESERVE =	0x10000;
/** Data is being appended, don't split full pages. */
public static final int MDB_APPEND =	0x20000;
/** Duplicate data is being appended, don't split full pages. */
public static final int MDB_APPENDDUP =	0x40000;
/** Store multiple data items in one call. Only for #MDB_DUPFIXED. */
public static final int MDB_MULTIPLE =	0x80000;
/*	@} */

/**	\defgroup mdb_copy	Copy Flags
 *	\{
 */
/** Compacting copy: Omit free space from copy, and renumber all
 * pages sequentially.
 */
public static final int MDB_CP_COMPACT =	0x01;
/*	@} */

/** \brief Cursor Get operations.
 *
 *	This is the set of all operations for retrieving data
 *	using a cursor.
 */
/** enum MDB_cursor_op */
public static final int
	/** Position at first key/data item */
	MDB_FIRST = 0,
	/** Position at first data item of current key.
								Only for #MDB_DUPSORT */
	MDB_FIRST_DUP = 1,
	/** Position at key/data pair. Only for #MDB_DUPSORT */
	MDB_GET_BOTH = 2,
	/** position at key, nearest data. Only for #MDB_DUPSORT */
	MDB_GET_BOTH_RANGE = 3,
	/** Return key/data at current cursor position */
	MDB_GET_CURRENT = 4,
	/** Return key and up to a page of duplicate data items
								from current cursor position. Move cursor to prepare
								for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
	MDB_GET_MULTIPLE = 5,
	/** Position at last key/data item */
	MDB_LAST = 6,
	/** Position at last data item of current key.
								Only for #MDB_DUPSORT */
	MDB_LAST_DUP = 7,
	/** Position at next data item */
	MDB_NEXT = 8,
	/** Position at next data item of current key.
								Only for #MDB_DUPSORT */
	MDB_NEXT_DUP = 9,
	/** Return key and up to a page of duplicate data items
								from next cursor position. Move cursor to prepare
								for #MDB_NEXT_MULTIPLE. Only for #MDB_DUPFIXED */
	MDB_NEXT_MULTIPLE = 10,
	/** Position at first data item of next key */
	MDB_NEXT_NODUP = 11,
	/** Position at previous data item */
	MDB_PREV = 12,
	/** Position at previous data item of current key.
								Only for #MDB_DUPSORT */
	MDB_PREV_DUP = 13,
	/** Position at last data item of previous key */
	MDB_PREV_NODUP = 14,
	/** Position at specified key */
	MDB_SET = 15,
	/** Position at specified key, return key + data */
	MDB_SET_KEY = 16,
	/** Position at first key greater than or equal to specified key. */
	MDB_SET_RANGE = 17;

/** \defgroup  errors	Return Codes
 *
 *	BerkeleyDB uses -30800 to -30999, we'll go under them
 *	\{
 */
	/**	Successful result */
public static final int MDB_SUCCESS =	 0;
	/** key/data pair already exists */
public static final int MDB_KEYEXIST =	(-30799);
	/** key/data pair not found (EOF) */
public static final int MDB_NOTFOUND =	(-30798);
	/** Requested page not found - this usually indicates corruption */
public static final int MDB_PAGE_NOTFOUND =	(-30797);
	/** Located page was wrong type */
public static final int MDB_CORRUPTED =	(-30796);
	/** Update of meta page failed or environment had fatal error */
public static final int MDB_PANIC =		(-30795);
	/** Environment version mismatch */
public static final int MDB_VERSION_MISMATCH =	(-30794);
	/** File is not a valid LMDB file */
public static final int MDB_INVALID =	(-30793);
	/** Environment mapsize reached */
public static final int MDB_MAP_FULL =	(-30792);
	/** Environment maxdbs reached */
public static final int MDB_DBS_FULL =	(-30791);
	/** Environment maxreaders reached */
public static final int MDB_READERS_FULL =	(-30790);
	/** Too many TLS keys in use - Windows only */
public static final int MDB_TLS_FULL =	(-30789);
	/** Txn has too many dirty pages */
public static final int MDB_TXN_FULL =	(-30788);
	/** Cursor stack too deep - internal error */
public static final int MDB_CURSOR_FULL =	(-30787);
	/** Page has not enough space - internal error */
public static final int MDB_PAGE_FULL =	(-30786);
	/** Database contents grew beyond environment mapsize */
public static final int MDB_MAP_RESIZED =	(-30785);
	/** Operation and DB incompatible, or DB type changed. This can mean:
	 *	<ul>
	 *	<li>The operation expects an #MDB_DUPSORT / #MDB_DUPFIXED database.
	 *	<li>Opening a named DB when the unnamed DB has #MDB_DUPSORT / #MDB_INTEGERKEY.
	 *	<li>Accessing a data record as a database, or vice versa.
	 *	<li>The database was dropped and recreated with different flags.
	 *	</ul>
	 */
public static final int MDB_INCOMPATIBLE =	(-30784);
	/** Invalid reuse of reader locktable slot */
public static final int MDB_BAD_RSLOT =		(-30783);
	/** Transaction must abort, has a child, or is invalid */
public static final int MDB_BAD_TXN =			(-30782);
	/** Unsupported size of key/DB name/data, or wrong DUPFIXED size */
public static final int MDB_BAD_VALSIZE =		(-30781);
	/** The specified DBI was changed unexpectedly */
public static final int MDB_BAD_DBI =		(-30780);
	/** The last defined error code */
public static final int MDB_LAST_ERRCODE =	MDB_BAD_DBI;
/** \} */
  
}
