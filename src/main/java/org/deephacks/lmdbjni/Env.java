package org.deephacks.lmdbjni;

import static org.deephacks.lmdbjni.LMDB.*;
import static org.deephacks.lmdbjni.Util.checkRc;
import static org.deephacks.lmdbjni.LMDB.mdb_env_open;

public class Env {
  private MDB_env env;

  public Env(String path) {
    this(path, 10485760);
  }

  public Env(String path, long size) {
    env = new MDB_env();
    checkRc(mdb_env_create(env));
    checkRc(mdb_env_set_maxreaders(env, 1));
    checkRc(mdb_env_set_mapsize(env, size));
    checkRc(mdb_env_set_maxdbs(env, 4));
    checkRc(mdb_env_open(env, path, MDB_WRITEMAP, 0664));
  }

  public Transaction openWriteTx() {
    MDB_txn tx = new MDB_txn();
    checkRc(mdb_txn_begin(env, null, 0, tx));
    return new Transaction(tx);
  }

  public Database openDatabase(Transaction tx, String name) {
    int dbi[] = new int[1];
    checkRc(mdb_dbi_open(tx.tx, name, MDB_CREATE, dbi));
    return new Database(dbi);
  }
}
