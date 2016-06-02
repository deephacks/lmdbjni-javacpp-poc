package org.deephacks.lmdbjni;

import org.agrona.concurrent.UnsafeBuffer;
import org.bytedeco.javacpp.Pointer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.deephacks.lmdbjni.LMDB.mdb_get;
import static org.deephacks.lmdbjni.LMDB.mdb_put;

public class Database {
  int[] dbi;

  public Database(int[] dbi) {
    this.dbi = dbi;
  }

  public void put(Transaction tx, byte[] key, byte[] val) {
    LMDB.MDB_val k = allocate(key);
    LMDB.MDB_val v = allocate(val);
    Util.checkRc(mdb_put(tx.tx, dbi[0], k, v, 0));
  }

  public byte[] get(Transaction tx, byte[] key) {
    LMDB.MDB_val k = allocate(key);
    LMDB.MDB_val result = new LMDB.MDB_val();
    Util.checkRc(mdb_get(tx.tx, dbi[0], k, result));
    UnsafeBuffer buffer = new UnsafeBuffer(result.address(), 16);

    long size = buffer.getLong(0, ByteOrder.LITTLE_ENDIAN);
    long address = buffer.getLong(8, ByteOrder.LITTLE_ENDIAN);

    buffer.wrap(address, (int) size);
    byte[] value = new byte[(int) size];
    buffer.getBytes(0, value);
    return value;
  }

  private static LMDB.MDB_val allocate(byte[] value) {
    ByteBuffer bb = ByteBuffer.allocateDirect(value.length);
    bb.put(value).flip();
    LMDB.MDB_val val = new LMDB.MDB_val();
    val.mv_size(value.length);
    val.mv_data(new Pointer(bb));
    return val;
  }
}
