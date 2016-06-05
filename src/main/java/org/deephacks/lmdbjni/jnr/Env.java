package org.deephacks.lmdbjni.jnr;

import jnr.ffi.Memory;
import static jnr.ffi.NativeType.ADDRESS;
import jnr.ffi.Pointer;
import static org.deephacks.lmdbjni.jnr.Library.MDB_WRITEMAP;
import static org.deephacks.lmdbjni.jnr.Library.lib;
import static org.deephacks.lmdbjni.jnr.Library.runtime;
import static org.deephacks.lmdbjni.jnr.Util.checkRc;

public class Env {

  final Pointer ptr;
  
  public Env(String path, long size) {
    Pointer envPtr = Memory.allocateDirect(runtime, ADDRESS);
    checkRc(lib.mdb_env_create(envPtr));

    ptr = envPtr.getPointer(0);
    
    checkRc(lib.mdb_env_set_maxreaders(ptr, 1));
    checkRc(lib.mdb_env_set_mapsize(ptr, size));
    checkRc(lib.mdb_env_set_maxdbs(ptr, 4));
    checkRc(lib.mdb_env_open(ptr, path, MDB_WRITEMAP, 0664));
  }
  
  public Transaction openWriteTx() {
    return new Transaction(this);
  }

  public Database openDatabase(Transaction tx, String name) {
    return new Database(tx, name);
  }

  
}
