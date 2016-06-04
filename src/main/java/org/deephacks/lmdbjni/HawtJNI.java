package org.deephacks.lmdbjni;

import org.fusesource.lmdbjni.*;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import static java.lang.Integer.BYTES;
import static java.lang.Math.max;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class HawtJNI {
  org.fusesource.lmdbjni.Env env;
  org.fusesource.lmdbjni.Transaction tx;
  org.fusesource.lmdbjni.Database db1;
  final DirectBuffer mdb = new DirectBuffer(0, 0);

  public HawtJNI(String path) {
    env = new org.fusesource.lmdbjni.Env(path);
    tx = env.createWriteTransaction();
    db1 = env.openDatabase(tx, "db1", Constants.CREATE);
  }

  public void insertData(final int opCount) {
    final ByteBuffer key = allocateDirect(BYTES);
    key.order(LITTLE_ENDIAN);

    final ByteBuffer val = allocateDirect(BYTES * 4);
    val.order(LITTLE_ENDIAN);

    for (int k = 0; k <= opCount; k++) {
      key.clear();
      val.clear();
      key.putInt(k).flip();
      val.putInt(k).putInt(k).putInt(k).putInt(k).flip();
      db1.put(tx, new DirectBuffer(key), new DirectBuffer(val));
    }
  }


  public void commit() {
    tx.commit();
  }

  public long crcViaDirectBuffer() {
    Cursor cursor = db1.openCursor(tx);

    final CRC32 crc32 = new CRC32();
    DirectBuffer k = new DirectBuffer(0, 0);
    DirectBuffer v = new DirectBuffer(0, 0);

    while (cursor.position(k, v, GetOp.NEXT) == 0) {
      assert k.capacity() > 0;
      assert v.capacity() > 0;

      final long biggest = max(k.capacity(), v.capacity());
      final byte[] scratch = new byte[(int) biggest];

      k.getBytes(0, scratch);
      crc32.update(scratch, 0, k.capacity());

      v.getBytes(0, scratch);
      crc32.update(scratch, 0, v.capacity());
    }
    return crc32.getValue();
  }
}
