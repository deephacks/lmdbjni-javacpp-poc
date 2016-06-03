package org.deephacks.lmdbjni;

import java.io.File;
import java.io.IOException;
import static java.lang.Integer.BYTES;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class LMDBTest {

  static {
    System.setProperty("java.library.path", "./target/classes");
  }

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final String DB_NAME = "db1";
  private Env env;

  @Before
  public void before() throws IOException {
    final File path = tmp.newFolder();
    env = new Env(path.getAbsolutePath());
  }

  @Test
  public void testGetPutByteArray() {
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    final byte[] key = new byte[]{1};
    final byte[] val = new byte[]{42};
    db1.put(tx, key, val);
    final byte[] fetchedVal = db1.get(tx, key);
    assertThat(fetchedVal, is(val));
    tx.commit();
  }
  
  @Test
  public void testGetPutByteBuffer() {
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    final ByteBuffer key = ByteBuffer.allocateDirect(BYTES);
    key.order(LITTLE_ENDIAN);
    assertThat(key.order(), is(LITTLE_ENDIAN));
    key.putInt(1).flip();

    final ByteBuffer val = ByteBuffer.allocateDirect(BYTES);
    val.order(LITTLE_ENDIAN);
    assertThat(val.order(), is(LITTLE_ENDIAN));
    val.putInt(42).flip();
    val.order(ByteOrder.LITTLE_ENDIAN);

    db1.put(tx, key, val);
    final ByteBuffer fetchedVal = db1.get(tx, key);
    assertThat(fetchedVal.order(), is(LITTLE_ENDIAN));
    assertThat(fetchedVal.position(), is(0));
    assertThat(fetchedVal.limit(), is(val.capacity()));
    assertThat(fetchedVal.capacity(), is(val.capacity()));
    assertThat(fetchedVal.getInt(0),is(42));
    tx.commit();
  }
}
