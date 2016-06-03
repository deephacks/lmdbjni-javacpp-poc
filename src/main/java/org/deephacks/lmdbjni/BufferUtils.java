package org.deephacks.lmdbjni;

import java.lang.reflect.Field;
import java.nio.Buffer;
import java.nio.ByteBuffer;

abstract class BufferUtils {

  private static final Field ADDR_FIELD;

  static {
    try {
      ADDR_FIELD = Buffer.class.getDeclaredField("address");
      ADDR_FIELD.setAccessible(true);
    } catch (NoSuchFieldException nfse) {
      throw new ExceptionInInitializerError(nfse);
    }
  }

  public static void setByteBufferAddress(ByteBuffer bb, long address) {
    try {
      ADDR_FIELD.setLong(bb, address);
    } catch (IllegalArgumentException | IllegalAccessException ex) {
      throw new UnsupportedOperationException(ex);
    }
  }
}
