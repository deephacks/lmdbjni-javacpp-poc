package org.deephacks.lmdbjni.jnr;

import java.io.File;
import java.io.IOException;
import static java.lang.Integer.BYTES;
import static java.lang.System.nanoTime;
import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class JnrTest {

  private static final int KEY_COUNT = 1_000;
  private static final int RUNS = 10_000;
  private static final int WARM = RUNS / 10;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final String DB_NAME = "db1";
  private Env env;

  @Before
  public void before() throws IOException {
    final File path = tmp.newFolder();
    final String p = path.getAbsolutePath();
    env = new Env(p, 1024 * 1024 * 1024);    
  }

  @Test
  public void bufferGetPut() {
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    final ByteBuffer key = allocateDirect(BYTES);
    key.order(LITTLE_ENDIAN);
    key.putInt(1).flip();

    final ByteBuffer val = allocateDirect(BYTES);
    val.order(LITTLE_ENDIAN);
    val.putInt(42).flip();

    db1.put(tx, key, val);
    final ByteBuffer fetchedVal = db1.get(tx, key);
    assertThat(fetchedVal.order(), is(LITTLE_ENDIAN));
    assertThat(fetchedVal.position(), is(0));
    assertThat(fetchedVal.limit(), is(val.capacity()));
    assertThat(fetchedVal.capacity(), is(val.capacity()));
    assertThat(fetchedVal.getInt(0), is(42));
    tx.commit();
  }

  @Test
  public void testCrc32ByByteBufferReflection() {
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    db1.insertData(tx, db1, KEY_COUNT);

    // run the cursor speed test (hacky: move to JMH)
    long start = 0;
    int sum = 0;
    for (int i = 0; i < RUNS; i++) {
      if (i == WARM) {
        start = nanoTime();
      }
      sum += db1.crc(tx);
    }
    final long finish = nanoTime();
    final long runtime = finish - start;
    report("JNR Reuse", runtime, sum);
    tx.commit();
  }
  
  private void report(String name, long nanos, int csum) {
    System.out.println(name + ": " + MILLISECONDS.convert(nanos, NANOSECONDS) + "\t" + csum);
  }


}
