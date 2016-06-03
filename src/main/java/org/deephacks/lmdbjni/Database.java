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
    MDB_val k = allocateMDB_val(key);
    MDB_val v = allocateMDB_val(val);
    checkRc(mdb_put(tx.tx, dbi[0], k, v, 0));
  }

  public void put(Transaction tx, ByteBuffer key, ByteBuffer val) {
    MDB_val k = new MDB_val();
    k.mv_size(key.capacity());
    k.mv_data(new Pointer(key));
    MDB_val v = new MDB_val(new Pointer(val));
    v.mv_size(val.capacity());
    v.mv_data(new Pointer(val));
    checkRc(mdb_put(tx.tx, dbi[0], k, v, 0));
  }

  public ByteBuffer get(Transaction tx, ByteBuffer key) {
    MDB_val k = new MDB_val();
    k.mv_size(key.capacity());
    k.mv_data(new Pointer(key));
    MDB_val result = new MDB_val();
    checkRc(mdb_get(tx.tx, dbi[0], k, result));
    // TODO:
    // MDB_val.data points to an address ...
    // how to wrap it in a ByteBuffer?

    // this does not work.. Maybe tweak InfoMapper to return a PointerPointer?
    Pointer mv_data = result.mv_data();
    System.out.println("Address is " + mv_data.address());
    System.out.println("Position is " + mv_data.position());
    System.out.println("Limit (original) is " + mv_data.limit());

    final long size = result.mv_size();
    System.out.println("mv_data set to mv_size...");
    mv_data.limit(size);
    System.out.println("Limit (new) is " + mv_data.limit());
    
    return mv_data.asByteBuffer();
  }

  public byte[] get(Transaction tx, byte[] key) {
    MDB_val k = allocateMDB_val(key);
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

  private static MDB_val allocateMDB_val(byte[] value) {
    ByteBuffer bb = ByteBuffer.allocateDirect(value.length);
    bb.put(value).flip();
    MDB_val val = new MDB_val();
    val.mv_size(value.length);
    val.mv_data(new Pointer(bb));
    return val;
  }

}
