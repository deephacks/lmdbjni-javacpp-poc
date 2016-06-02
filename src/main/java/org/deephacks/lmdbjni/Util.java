package org.deephacks.lmdbjni;

import static org.deephacks.lmdbjni.LMDB.mdb_strerror;

class Util {
  static void checkRc(int rc) {
    if (rc != 0) {
      throw new IllegalArgumentException("error " + rc + " " + mdb_strerror(rc).getString());
    }
  }
}
