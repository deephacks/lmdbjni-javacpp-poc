package org.deephacks.lmdbjni;

import java.io.File;
import java.io.IOException;
import static java.lang.Integer.BYTES;
import static java.lang.System.nanoTime;
import static java.lang.System.setOut;
import static java.lang.System.setProperty;

import org.junit.Test;

import java.nio.ByteBuffer;
import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import org.agrona.concurrent.UnsafeBuffer;
import org.fusesource.lmdbjni.DirectBuffer;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

public class LMDBTest {

  private static final int KEY_COUNT = 1_000;
  private static final int RUNS = 10_000;
  private static final int WARM = RUNS / 10;
  
  static {
    System.setProperty(UnsafeBuffer.DISABLE_BOUNDS_CHECKS_PROP_NAME, "true");
    System.setProperty(DirectBuffer.DISABLE_BOUNDS_CHECKS_PROP_NAME, "true");
  }

  static {
    setProperty("java.library.path", "./target/classes");
  }

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  private final String DB_NAME = "db1";
  private Env env;
  File hawtjnipath;

  @Before
  public void before() throws IOException {
    File path = tmp.newFolder();
    env = new Env(path.getAbsolutePath());
    this.hawtjnipath = tmp.newFolder();
    env = new Env(path.getAbsolutePath());
  }

  @Test
  public void testCrc32ByByteBufferReflection() {
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    db1.insertData(tx, db1, KEY_COUNT);

    // check the CRCs are symmetrical
    final long crcFromDb = db1.crcViaDirectBuffer(tx);
    assertThat(db1.crcViaByteBufferReuse(tx), is(crcFromDb));

    // run the cursor speed test (hacky: move to JMH)
    long start = 0;
    int sum = 0;
    for (int i = 0; i < RUNS; i++) {
      if (i == WARM) {
        start = nanoTime();
      }
      sum += db1.crcViaByteBufferReuse(tx);
    }
    final long finish = nanoTime();
    final long runtime = finish - start;
    report("BB Reuse", runtime, sum);
    tx.commit();
  }
  
  private void report(String name, long nanos, int csum) {
    System.out.println(name + ": " + MILLISECONDS.convert(nanos, NANOSECONDS) + "\t" + csum);
  }

  @Test
  @Ignore
  public void testCrc32ByByteBufferSafe() {
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    db1.insertData(tx, db1, KEY_COUNT);

    // check the CRCs are symmetrical
    final long crcFromDb = db1.crcViaDirectBuffer(tx);
    assertThat(db1.crcViaByteBuffer(tx), is(crcFromDb));

    // run the cursor speed test (hacky: move to JMH)
    long start = 0;
    int sum = 0;
    for (int i = 0; i < RUNS; i++) {
      if (i == WARM) {
        start = nanoTime();
      }
      sum += db1.crcViaByteBuffer(tx);
    }
    final long finish = nanoTime();
    final long runtime = finish - start;
    report("BB Alloc", runtime, sum);
    tx.commit();
  }

  @Test
  @Ignore
  public void testCrc32ByDirectBuffer() {
    assertThat(UnsafeBuffer.SHOULD_BOUNDS_CHECK, is(false));
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    db1.insertData(tx, db1, KEY_COUNT);

    // check the CRCs are symmetrical
    final long crcFromDb = db1.crcViaDirectBuffer(tx);
    assertThat(db1.crcViaByteBuffer(tx), is(crcFromDb));

    // run the cursor speed test (hacky: move to JMH)
    long start = 0;
    int sum = 0;
    for (int i = 0; i < RUNS; i++) {
      if (i == WARM) {
        start = nanoTime();
      }
      sum += db1.crcViaDirectBuffer(tx);
    }
    final long finish = nanoTime();
    final long runtime = finish - start;
    report("DB Reuse", runtime, sum);
    tx.commit();
  }


  @Test
  public void testCrc32ByHawtJNI() {
    assertThat(DirectBuffer.SHOULD_BOUNDS_CHECK, is(false));
    final HawtJNI hawtJNI = new HawtJNI(hawtjnipath.getAbsolutePath());
    hawtJNI.insertData(KEY_COUNT);

    // check the CRCs are symmetrical
    final Transaction tx = env.openWriteTx();
    final Database db1 = env.openDatabase(tx, DB_NAME);
    db1.insertData(tx, db1, KEY_COUNT);
    final long crcFromBb = db1.crcViaByteBufferReuse(tx);
    assertThat(hawtJNI.crcViaDirectBuffer(), is(crcFromBb));
    tx.commit();

    // run the cursor speed test (hacky: move to JMH)
    long start = 0;
    int sum = 0;
    for (int i = 0; i < RUNS; i++) {
      if (i == WARM) {
        start = nanoTime();
      }
      sum += hawtJNI.crcViaDirectBuffer();
    }
    final long finish = nanoTime();
    final long runtime = finish - start;
    report("HawtJNI ", runtime, sum);
    hawtJNI.commit();
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
}
