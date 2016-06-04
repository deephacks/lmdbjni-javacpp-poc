package org.deephacks.lmdbjni.jnr;

import static java.lang.Integer.BYTES;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import java.nio.ByteOrder;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import java.util.zip.CRC32;
import jnr.ffi.Memory;
import jnr.ffi.NativeType;
import static jnr.ffi.NativeType.ADDRESS;
import static jnr.ffi.NativeType.UINT;
import jnr.ffi.Pointer;
import jnr.ffi.Struct;
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
    Pointer dbiPtr = Memory.allocate(runtime, UINT);
    checkRc(lib.mdb_dbi_open(tx.ptr, name, MDB_CREATE, dbiPtr));
    dbi = dbiPtr.getInt(0);
  }

  public void put(Transaction tx, ByteBuffer key, ByteBuffer val) {
    final MDB_val k = new MDB_val(runtime);
    k.size.set(key.limit());
    k.data.set(new ByteBufferMemoryIO(runtime, key));

    final MDB_val v = new MDB_val(runtime);
    v.size.set(val.limit());
    v.data.set(new ByteBufferMemoryIO(runtime, val));

    checkRc(lib.mdb_put(tx.ptr, dbi, k, v, 0));
  }

  public ByteBuffer get(Transaction tx, ByteBuffer key) {
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

  final ByteBuffer keyBb = allocateDirect(1);
  final ByteBuffer valBb = allocateDirect(1);

  public long crc(Transaction tx) {
    Pointer cursorPtr = Memory.allocate(runtime, ADDRESS);
    checkRc(lib.mdb_cursor_open(tx.ptr, dbi, cursorPtr));
    final Pointer cursor = cursorPtr.getPointer(0);

    final MDB_val k = new MDB_val(runtime);
    final MDB_val v = new MDB_val(runtime);

    final CRC32 crc32 = new CRC32();
    while (lib.mdb_cursor_get(cursor, k, v, MDB_NEXT) == 0) {
      assert k.size.get() > 0;
      assert v.size.get() > 0;

      MemoryAccess.wrap(keyBb, k.data.get().address(), (int)k.size.get());
      MemoryAccess.wrap(valBb, v.data.get().address(), (int)v.size.get());

      crc32.update(keyBb);
      crc32.update(valBb);
    }
    return crc32.getValue();
  }

}
