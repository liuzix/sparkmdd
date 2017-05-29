import java.lang.reflect.Field;
import sun.misc.Unsafe;
import java.util.*;


public final class UnsafeWrapper {

  // this is the jvm unsafe object, visible only within the class
  private static final Unsafe _UNSAFE;

  static {
    // grab the unsafe class during load time using static call
    sun.misc.Unsafe unsafe;
    try {
      Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
      unsafeField.setAccessible(true);
      unsafe = (sun.misc.Unsafe) unsafeField.get(null);
    } catch (Throwable cause) {
      unsafe = null;
    }
    _UNSAFE = unsafe;

    if (_UNSAFE != null) {
      BYTE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(byte[].class);
      INT_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(int[].class);
      LONG_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(long[].class);
      DOUBLE_ARRAY_OFFSET = _UNSAFE.arrayBaseOffset(double[].class);
    } else {
      BYTE_ARRAY_OFFSET = 0;
      INT_ARRAY_OFFSET = 0;
      LONG_ARRAY_OFFSET = 0;
      DOUBLE_ARRAY_OFFSET = 0;
    }
  }

  public static final int BYTE_ARRAY_OFFSET;
  public static final int INT_ARRAY_OFFSET;
  public static final int LONG_ARRAY_OFFSET;
  public static final int DOUBLE_ARRAY_OFFSET;
  private static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  public static final class UNSAFE {

    private UNSAFE() { }

    public static int getInt(Object object, long offset) {
      return _UNSAFE.getInt(object, offset);
    }

    public static void putInt(Object object, long offset, int value) {
      _UNSAFE.putInt(object, offset, value);
    }

    public static boolean getBoolean(Object object, long offset) {
      return _UNSAFE.getBoolean(object, offset);
    }

    public static void putBoolean(Object object, long offset, boolean value) {
      _UNSAFE.putBoolean(object, offset, value);
    }

    public static byte getByte(Object object, long offset) {
      return _UNSAFE.getByte(object, offset);
    }

    public static void putByte(Object object, long offset, byte value) {
      _UNSAFE.putByte(object, offset, value);
    }

    public static short getShort(Object object, long offset) {
      return _UNSAFE.getShort(object, offset);
    }

    public static void putShort(Object object, long offset, short value) {
      _UNSAFE.putShort(object, offset, value);
    }

    public static long getLong(Object object, long offset) {
      return _UNSAFE.getLong(object, offset);
    }

    public static void putLong(Object object, long offset, long value) {
      _UNSAFE.putLong(object, offset, value);
    }

    public static float getFloat(Object object, long offset) {
      return _UNSAFE.getFloat(object, offset);
    }

    public static void putFloat(Object object, long offset, float value) {
      _UNSAFE.putFloat(object, offset, value);
    }

    public static double getDouble(Object object, long offset) {
      return _UNSAFE.getDouble(object, offset);
    }

    public static void putDouble(Object object, long offset, double value) {
      _UNSAFE.putDouble(object, offset, value);
    }

    public static long allocateMemory(long size) {
      return _UNSAFE.allocateMemory(size);
    }

    public static void freeMemory(long address) {
      _UNSAFE.freeMemory(address);
    }

  }

  static public void copyMemory(
      Object src,
      long srcOffset,
      Object dst,
      long dstOffset,
      long length) {
    while (length > 0) {
      long size = Math.min(length, UNSAFE_COPY_THRESHOLD);
      _UNSAFE.copyMemory(src, srcOffset, dst, dstOffset, size);
      length -= size;
      srcOffset += size;
      dstOffset += size;
    }
  }

  /**
   * Raises an exception bypassing compiler checks for checked exceptions.
   */
  public static void throwException(Throwable t) {
    _UNSAFE.throwException(t);
  }


/*
@Test
public void testDirectIntArray() throws Exception {
  long maximum = Integer.MAX_VALUE + 1L;
  DirectIntArray directIntArray = new DirectIntArray(maximum);
  directIntArray.setValue(0L, 10);
  directIntArray.setValue(maximum, 20);
  assertEquals(10, directIntArray.getValue(0L));
  assertEquals(20, directIntArray.getValue(maximum));
  directIntArray.destroy();
}*/


  public static void test(String[] args) throws InstantiationException {
    /* let us try to play with this */
    DirectIntArray directIntArray = new DirectIntArray(100);

    for (int i = 0; i < 100; i++) {
      directIntArray.setValue(i, 10 * i);
    }

    System.out.print ("%%%%%% Finish setting\n");

    for (int i = 0; i < 100; i++) {
      System.out.format("At index %d, val is %d\n", i, directIntArray.getValue(i));
    }
    directIntArray.destroy();
  }
}



class DirectIntArray implements Iterable<Integer> {

  private final static long INT_SIZE_IN_BYTES = 4;

  private final long startIndex;

  private final long size;

  public long getIndex() {return startIndex;}

  private long index(long offset) {
    return startIndex + offset * INT_SIZE_IN_BYTES;
  }

  public DirectIntArray(long size) {
    this.size = size;
    startIndex = UnsafeWrapper.UNSAFE.allocateMemory(size * INT_SIZE_IN_BYTES);
    // unsafe.setMemory(startIndex, size * INT_SIZE_IN_BYTES, (byte) 0);
  }

  public void setValue(long index, int value) {
    UnsafeWrapper.UNSAFE.putInt(null, index(index), value);
  }

  public int getValue(long index) {
    return UnsafeWrapper.UNSAFE.getInt(null, index(index));
  }

  public void destroy() {
    UnsafeWrapper.UNSAFE.freeMemory(startIndex);
  }


  class UnsafeIterator implements Iterator<Integer> {
      int current = 0;
      public boolean hasNext() {
          if (current < DirectIntArray.this.size) {
              return true;
          } else {
              return false;
          }
      }
      public Integer next() {
          if (!hasNext()) {
              throw new NoSuchElementException();
          }
          return (Integer)(DirectIntArray.this.getValue(current++));
      }
  }

/*  public PrimitiveIterator.OfInt iterator()
  {
    return 
  }*/

  public Iterator<Integer> iterator() {
      return new UnsafeIterator();
  }

}