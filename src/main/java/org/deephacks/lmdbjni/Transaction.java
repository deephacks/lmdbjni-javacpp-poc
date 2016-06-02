package org.deephacks.lmdbjni;

import static org.deephacks.lmdbjni.Util.checkRc;

public class Transaction {
  LMDB.MDB_txn tx;

  Transaction(LMDB.MDB_txn tx) {
    this.tx = tx;
  }

  public void commit() {
    checkRc(LMDB.mdb_txn_commit(tx));
  }
}
