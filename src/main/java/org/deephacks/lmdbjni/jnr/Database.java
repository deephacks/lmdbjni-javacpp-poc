package org.deephacks.lmdbjni.jnr;

import static java.lang.Integer.BYTES;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import java.util.zip.CRC32;
import jnr.ffi.Memory;
import static jnr.ffi.NativeType.ADDRESS;
import static jnr.ffi.NativeType.UINT;
import jnr.ffi.Pointer;
import jnr.ffi.provider.jffi.ByteBufferMemoryIO;
import org.deephacks.lmdbjni.MemoryAccess;
import static org.deephacks.lmdbjni.jnr.Library.MDB_CREATE;
import static org.deephacks.lmdbjni.jnr.Library.MDB_NEXT;
import org.deephacks.lmdbjni.jnr.Library.MDB_val;
import static org.deephacks.lmdbjni.jnr.Library.lib;
import static org.deephacks.lmdbjni.jnr.Library.runtime;
import static org.deephacks.lmdbjni.jnr.Util.checkRc;

public class Database {

  final int dbi;

  Database(Transaction tx, String name) {
    Pointer dbiPtr = Memory.allocateDirect(runtime, UINT);
    checkRc(lib.mdb_dbi_open(tx.ptr, name, MDB_CREATE, dbiPtr));
    dbi = dbiPtr.getInt(0);
  }

  public void put(Transaction tx, ByteBuffer key, ByteBuffer val) {
    assert key.isDirect();
    assert val.isDirect();

    final MDB_val k = new MDB_val(runtime);
    k.size.set(key.limit());
    k.data.set(new ByteBufferMemoryIO(runtime, key));

    final MDB_val v = new MDB_val(runtime);
    v.size.set(val.limit());
    v.data.set(new ByteBufferMemoryIO(runtime, val));

    checkRc(lib.mdb_put(tx.ptr, dbi, k, v, 0));
  }

  public ByteBuffer get(Transaction tx, ByteBuffer key) {
    assert key.isDirect();
    
    final MDB_val k = new MDB_val(runtime);
    k.size.set(key.limit());
    k.data.set(new ByteBufferMemoryIO(runtime, key));

    final MDB_val v = new MDB_val(runtime);
    checkRc(lib.mdb_get(tx.ptr, dbi, k, v));

    final long size = v.size.get();
    // so this is inefficient as we go create a BB, but if you need performance you should be using a cursor in the first place
    ByteBuffer bb = ByteBuffer.allocateDirect(1).order(LITTLE_ENDIAN);
    MemoryAccess.wrap(bb, v.data.get().address(), (int) size);
    return bb;
  }

  public void insertData(final Transaction tx, final Database db,
                         final int opCount) {
    final ByteBuffer key = allocateDirect(BYTES);
    key.order(LITTLE_ENDIAN);

    final ByteBuffer val = allocateDirect(BYTES * 4);
    val.order(LITTLE_ENDIAN);

    for (int k = 0; k <= opCount; k++) {
      key.clear();
      val.clear();
      key.putInt(k).flip();
      val.putInt(k).putInt(k).putInt(k).putInt(k).flip();
      db.put(tx, key, val);
    }
  }

  final ByteBuffer keyBb = allocateDirect(0);
  final ByteBuffer valBb = allocateDirect(0);
  final int mdbValSize = Long.BYTES + Long.BYTES;

  public static long wrap(ByteBuffer buffer, Pointer mdbVal) {
    // struct MDB_val { size_t mv_size; void *mv_data; }
    // next line won't work as impl returns 0. so no unsafe-accelerated path...
    //final long addr = mdbVal.address();
    //assert addr != 0;
    final long size = mdbVal.getLong(0);
    final long data = mdbVal.getAddress(Long.BYTES);
    MemoryAccess.wrap(buffer, data, (int) size);
    return size;
  }
  
  public long crc(Transaction tx) {
    Pointer cursorPtr = Memory.allocateDirect(runtime, ADDRESS);
    checkRc(lib.mdb_cursor_open(tx.ptr, dbi, cursorPtr));
    final Pointer cursor = cursorPtr.getPointer(0);

    final Pointer k = Memory.allocateDirect(runtime, mdbValSize);
    final Pointer v = Memory.allocateDirect(runtime, mdbValSize);

    final CRC32 crc32 = new CRC32();
    while (lib.mdb_cursor_get(cursor, k, v, MDB_NEXT) == 0) {
      final long kSize = wrap(keyBb,  k);
      final long vSize = wrap(valBb,  v);
      assert kSize > 0;
      assert vSize > 0;
      crc32.update(keyBb);
      crc32.update(valBb);
    }
    return crc32.getValue();
  }

}
