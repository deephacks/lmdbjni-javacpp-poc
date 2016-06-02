package org.deephacks.lmdbjni;

import org.agrona.concurrent.UnsafeBuffer;
import org.bytedeco.javacpp.Pointer;
import static org.deephacks.lmdbjni.LMDB.*;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.deephacks.lmdbjni.LMDB.mdb_get;
import static org.deephacks.lmdbjni.LMDB.mdb_put;
import static org.deephacks.lmdbjni.Util.checkRc;

public class Database {
  int[] dbi;

  public Database(int[] dbi) {
    this.dbi = dbi;
  }

  public void put(Transaction tx, byte[] key, byte[] val) {
    MDB_val k = allocate(key);
    MDB_val v = allocate(val);
    checkRc(mdb_put(tx.tx, dbi[0], k, v, 0));
  }

  public byte[] get(Transaction tx, byte[] key) {
    MDB_val k = allocate(key);
    MDB_val result = new MDB_val();
    checkRc(mdb_get(tx.tx, dbi[0], k, result));
    UnsafeBuffer buffer = new UnsafeBuffer(result.address(), 16);

    long size = buffer.getLong(0, ByteOrder.LITTLE_ENDIAN);
    long address = buffer.getLong(8, ByteOrder.LITTLE_ENDIAN);

    buffer.wrap(address, (int) size);
    byte[] value = new byte[(int) size];
    buffer.getBytes(0, value);
    return value;
  }

  private static MDB_val allocate(byte[] value) {
    ByteBuffer bb = ByteBuffer.allocateDirect(value.length);
    bb.put(value).flip();
    MDB_val val = new MDB_val();
    val.mv_size(value.length);
    val.mv_data(new Pointer(bb));
    return val;
  }
}
