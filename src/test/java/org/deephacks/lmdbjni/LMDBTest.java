package org.deephacks.lmdbjni;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class LMDBTest {
  static {
    System.setProperty("java.library.path", "./target/classes");
  }

  @Test
  public void testGetPutByteArray() {
    Env env = new Env("/tmp/lmdb");
    Transaction tx = env.openWriteTx();
    Database db1 = env.openDatabase(tx, "db1");
    db1.put(tx, new byte[] {1}, new byte[] {4});
    byte[] bytes = db1.get(tx, new byte[]{1});
    System.out.println(Arrays.toString(bytes));
    tx.commit();
  }

  @Test
  public void testGetPutByteBuffer() {
    Env env = new Env("/tmp/lmdb");
    Transaction tx = env.openWriteTx();
    Database db1 = env.openDatabase(tx, "db1");
    ByteBuffer key = ByteBuffer.allocateDirect(4);
    key.putInt(1).flip();
    ByteBuffer val = ByteBuffer.allocateDirect(4);
    val.putInt(1).flip();
    db1.put(tx, key, val);
    ByteBuffer value = db1.get(tx, key);
    tx.commit();
  }
}

