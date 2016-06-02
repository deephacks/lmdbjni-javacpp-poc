package org.deephacks.lmdbjni;

import java.util.Arrays;

public class Main {
  public static void main(String[] args) {
    System.setProperty("java.library.path", "./target/classes");
    Env env = new Env("/tmp/lmdb");
    Transaction tx = env.openWriteTx();
    Database db1 = env.openDatabase(tx, "db1");
    db1.put(tx, new byte[] {1}, new byte[] {4});
    byte[] bytes = db1.get(tx, new byte[]{1});
    System.out.println(Arrays.toString(bytes));
    tx.commit();
  }
}

