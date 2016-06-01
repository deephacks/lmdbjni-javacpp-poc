package org.deephacks.lmdbjni;

import org.bytedeco.javacpp.IntPointer;
import org.bytedeco.javacpp.Pointer;
import static org.deephacks.lmdbjni.LMDB.*;

public class Main {
  public static void main(String[] args) {

    System.setProperty("java.library.path", "./target/classes");
    MDB_env env = new MDB_env();
    mdb_env_create(env);
    mdb_env_open(env, "/tmp/lmdb", 0, 0644);
    MDB_txn tx = new MDB_txn();
    mdb_txn_begin(env, tx, 1, tx);
    mdb_dbi_open(tx, "db", 1, new IntPointer());
    MDB_val val = new MDB_val();
    Pointer p = new Pointer();
    val.mv_data(p);
    mdb_put(tx, 1, val, val, 1);
  }
}

