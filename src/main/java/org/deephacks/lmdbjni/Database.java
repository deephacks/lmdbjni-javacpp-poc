package org.deephacks.lmdbjni;

import static java.lang.Integer.MAX_VALUE;
import org.bytedeco.javacpp.Pointer;
import static org.deephacks.lmdbjni.LMDB.*;

import java.nio.ByteBuffer;
import org.bytedeco.javacpp.BytePointer;

import static org.deephacks.lmdbjni.LMDB.mdb_get;
import static org.deephacks.lmdbjni.LMDB.mdb_put;
import static org.deephacks.lmdbjni.Util.checkRc;

public class Database {

  int[] dbi;

  public Database(int[] dbi) {
    this.dbi = dbi;
  }

  public void put(Transaction tx, byte[] key, byte[] val) {
    final MDB_val k = allocateMDB_val(key);
    final MDB_val v = allocateMDB_val(val);
    checkRc(mdb_put(tx.tx, dbi[0], k, v, 0));
  }

  public void put(Transaction tx, ByteBuffer key, ByteBuffer val) {
    final MDB_val k = new MDB_val();
    k.mv_size(key.limit());
    k.mv_data(new Pointer(key));

    final MDB_val v = new MDB_val();
    v.mv_size(val.limit());
    v.mv_data(new Pointer(val));

    checkRc(mdb_put(tx.tx, dbi[0], k, v, 0));
  }

  public ByteBuffer get(Transaction tx, ByteBuffer key) {
    final MDB_val k = new MDB_val();
    k.mv_size(key.limit());
    k.mv_data(new Pointer(key));

    final MDB_val v = new MDB_val();

    checkRc(mdb_get(tx.tx, dbi[0], k, v));
    final long size = v.mv_size();
    final Pointer mv_data = v.mv_data();
    mv_data.capacity(size); // also sets limit automatically

    assert mv_data.position() == 0;
    assert mv_data.limit() == size;
    assert mv_data.capacity() == size;
    return mv_data.asByteBuffer();
  }

  public byte[] get(Transaction tx, byte[] key) {
    MDB_val k = allocateMDB_val(key);

    MDB_val v = new MDB_val();

    checkRc(mdb_get(tx.tx, dbi[0], k, v));
    final long size = v.mv_size();
    if (size > MAX_VALUE) {
      throw new UnsupportedOperationException("Value too large for byte[]");
    }
    final Pointer mv_data = v.mv_data();
    mv_data.capacity(size);

    assert mv_data.position() == 0;
    assert mv_data.limit() == size;
    assert mv_data.capacity() == size;

    // forced copy to byte[]
    byte[] vBytes = new byte[(int) size];
    mv_data.asByteBuffer().get(vBytes);
    return vBytes;
  }

  private static MDB_val allocateMDB_val(byte[] value) {
    final BytePointer bp = new BytePointer(value);
    final MDB_val val = new MDB_val();
    val.mv_size(value.length);
    val.mv_data(bp);
    return val;
  }

}
