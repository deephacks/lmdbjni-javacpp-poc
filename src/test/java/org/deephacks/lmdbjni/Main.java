package org.deephacks.lmdbjni;

import org.agrona.concurrent.UnsafeBuffer;
import org.bytedeco.javacpp.BytePointer;
import org.bytedeco.javacpp.Pointer;

import java.awt.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.deephacks.lmdbjni.LMDB.*;

public class Main {
  public static void main(String[] args) {
    int flag = 0;
    int mode = 0664;
    int rc = 0;
    System.setProperty("java.library.path", "./target/classes");
    MDB_env env = new MDB_env();
    c(mdb_env_create(env));
    c(mdb_env_set_maxreaders(env, 1));
    c(mdb_env_set_mapsize(env, 10485760));
    c(mdb_env_set_maxdbs(env, 2));
    c(mdb_env_open(env, "/tmp/lmdb", flag, mode));
    System.out.println("max key size " + mdb_env_get_maxkeysize(env));
    MDB_txn tx = new MDB_txn();
    MDB_txn parent = null;
    c(mdb_txn_begin(env, parent, flag, tx));
    int dbi[] = new int[1];
    c(mdb_dbi_open(tx, (String) null, flag, dbi));
    System.out.println("dbi " + Arrays.toString(dbi));
    MDB_val key = val(1);
    MDB_val val = val(1);
    c(mdb_put(tx, dbi[0], key, val, flag));
    MDB_val result = new MDB_val();
    c(mdb_get(tx, dbi[0], val(2), result));

    Pointer pointer = result.mv_data();
    UnsafeBuffer buffer = new UnsafeBuffer(result.address(), 16);

    long size = buffer.getLong(0, ByteOrder.LITTLE_ENDIAN);
    System.out.println(size + " " + result.mv_size());

    long address = buffer.getLong(8, ByteOrder.LITTLE_ENDIAN);
    System.out.println(address + " " + pointer.address());

    UnsafeBuffer value = new UnsafeBuffer(pointer.address(), (int) size);
    System.out.println("val " + value.getInt(0, ByteOrder.LITTLE_ENDIAN));
    System.out.println("val " + value.getInt(0, ByteOrder.BIG_ENDIAN));
  }

  public static void c(int rc) {
    if (rc != 0) {
      throw new IllegalArgumentException("error " + rc);
    }
  }

  public static MDB_val val(int value) {
    ByteBuffer bVal = ByteBuffer.allocateDirect(4).order(ByteOrder.LITTLE_ENDIAN);
    bVal.putInt(value);
    MDB_val val = new MDB_val();
    val.mv_size(4);
    val.mv_data(new Pointer(bVal));
    return val;
  }
}

