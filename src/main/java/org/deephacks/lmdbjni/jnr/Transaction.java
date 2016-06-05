package org.deephacks.lmdbjni.jnr;

import jnr.ffi.Memory;
import static jnr.ffi.NativeType.ADDRESS;
import jnr.ffi.Pointer;
import static org.deephacks.lmdbjni.jnr.Library.lib;
import static org.deephacks.lmdbjni.jnr.Library.runtime;
import static org.deephacks.lmdbjni.jnr.Util.checkRc;

public class Transaction {

  final Pointer ptr;

  Transaction(Env env) {
    Pointer txnPtr = Memory.allocateDirect(runtime, ADDRESS);
    checkRc(lib.mdb_txn_begin(env.ptr, null, 0, txnPtr));
    ptr = txnPtr.getPointer(0);
  }
  
  public void commit() {
    checkRc(lib.mdb_txn_commit(ptr));
  }

}
