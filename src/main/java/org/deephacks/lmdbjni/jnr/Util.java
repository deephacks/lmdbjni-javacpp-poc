package org.deephacks.lmdbjni.jnr;

class Util {
  static void checkRc(int rc) {
    if (rc != 0) {
      throw new IllegalArgumentException("error " + rc);
    }
  }
}
