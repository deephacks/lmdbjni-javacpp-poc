package org.deephacks.lmdbjni;

import static java.lang.Integer.BYTES;
import static java.lang.Integer.MAX_VALUE;
import static java.lang.Math.max;
import org.bytedeco.javacpp.Pointer;
import static org.deephacks.lmdbjni.LMDB.*;

import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import java.util.zip.CRC32;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.bytedeco.javacpp.BytePointer;
import org.fusesource.lmdbjni.DirectBuffer;

import static org.deephacks.lmdbjni.BufferUtils.setByteBufferAddress;

import static org.deephacks.lmdbjni.LMDB.mdb_get;
import static org.deephacks.lmdbjni.LMDB.mdb_put;
import static org.deephacks.lmdbjni.Util.checkRc;
import static java.lang.Math.max;

public class Database {

  private static MDB_val allocateMDB_val(byte[] value) {
    final BytePointer bp = new BytePointer(value);
    final MDB_val val = new MDB_val();
    val.mv_size(value.length);
    val.mv_data(bp);
    return val;
  }

  int[] dbi;

  final ByteBuffer keyBb = allocateDirect(1);
  final MutableDirectBuffer mdb = new UnsafeBuffer(new byte[0]);
  final ByteBuffer valBb = allocateDirect(1);

  public Database(int[] dbi) {
    this.dbi = dbi;
  }

  public long crcViaByteBuffer(Transaction tx) {
    final MDB_cursor cursor = new MDB_cursor();
    checkRc(mdb_cursor_open(tx.tx, dbi[0], cursor));

    final MDB_val k = new MDB_val();
    final MDB_val v = new MDB_val();

    final CRC32 crc32 = new CRC32();
    while (mdb_cursor_get(cursor, k, v, MDB_NEXT) == 0) {
      assert k.mv_size() > 0;
      assert v.mv_size() > 0;

      final Pointer keyData = k.mv_data();
      keyData.capacity(k.mv_size());

      final Pointer valData = v.mv_data();
      valData.capacity(v.mv_size());

      final ByteBuffer keyBb = keyData.asByteBuffer();
      final ByteBuffer valBb = valData.asByteBuffer();
      crc32.update(keyBb);
      crc32.update(valBb);
    }
    return crc32.getValue();
  }

  public long crcViaByteBufferReuse(Transaction tx) {
    final MDB_cursor cursor = new MDB_cursor();
    checkRc(mdb_cursor_open(tx.tx, dbi[0], cursor));

    final MDB_val k = new MDB_val();
    final MDB_val v = new MDB_val();

    final CRC32 crc32 = new CRC32();
    while (mdb_cursor_get(cursor, k, v, MDB_NEXT) == 0) {
      assert k.mv_size() > 0;
      assert v.mv_size() > 0;

      MemoryAccess.wrap(keyBb, k.mv_data().address(), (int)k.mv_size());
      MemoryAccess.wrap(valBb, v.mv_data().address(), (int)v.mv_size());
      crc32.update(keyBb);
      crc32.update(valBb);
    }
    return crc32.getValue();
  }

  public long crcViaDirectBuffer(Transaction tx) {
    final MDB_cursor cursor = new MDB_cursor();
    checkRc(mdb_cursor_open(tx.tx, dbi[0], cursor));

    final MDB_val k = new MDB_val();
    final MDB_val v = new MDB_val();

    final CRC32 crc32 = new CRC32();

    while (mdb_cursor_get(cursor, k, v, MDB_NEXT) == 0) {
      assert k.mv_size() > 0;
      assert v.mv_size() > 0;

      final long biggest = max(k.mv_size(), v.mv_size());
      final byte[] scratch = new byte[(int) biggest];

      wrap(mdb, k);
      mdb.getBytes(0, scratch);
      crc32.update(scratch, 0, (int) k.mv_size());

      wrap(mdb, v);
      mdb.getBytes(0, scratch);
      crc32.update(scratch, 0, (int) v.mv_size());
    }
    return crc32.getValue();
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

  public void insertData(final Transaction tx, final Database db1,
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
      db1.put(tx, key, val);
    }
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

  private void wrap(final MutableDirectBuffer mdb, final MDB_val mdbVal) {
    final long size = mdbVal.mv_size();
    if (size > MAX_VALUE) {
      throw new UnsupportedOperationException("Value too large for Agrona");
    }
    mdb.wrap(mdbVal.mv_data().address(), (int) size);
  }

}
