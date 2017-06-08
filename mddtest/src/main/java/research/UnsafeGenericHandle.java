package research;

import java.io.*;

import static research.UnsafeWrapper.BYTE_ARRAY_OFFSET;


/*import java.util.Arrays;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;*/

// import java.nio.ByteBuffer;
// import com.esotericsoftware.kryo.Kryo;
// import com.esotericsoftware.kryo.KryoSerializable;
// import com.esotericsoftware.kryo.io.Input;
// import com.esotericsoftware.kryo.io.Output;
// import org.apache.spark.unsafe.hash.Murmur3_x86_32;



/**
 * [null bit set] [values] [variable length portion]
 *
 * The bit set is used for null tracking and is aligned to 8-byte word boundaries.  It stores
 * one bit per field.
 *
 * Store one 8-byte word per field. For fields that hold fixed-length
 * primitive types, such as long, double, or int, we store the value directly in the word.
 * Fields with variable lengths have not been implemented. But ideally the field should be a pointer
 * to unsafe mem allocated off-heap if possible.
 *
 */


public final class UnsafeGenericHandle implements java.io.Serializable {

  static MemoryAllocator manager = new MemoryPoolAllocator();

  /* round up to multiple of 8 bytes */
  public static int calculateBitSetWidthInBytes(int numFields) {
    return ((numFields + 63)/ 64) * 8;
  }

/*  public static int calculateFixedPortionByteSize(int index) {
    return 8 * index + calculateBitSetWidthInBytes(numFields);
  }*/

  private MemorySegment mem = null;

  private Object baseObject;
  public Object getBaseObject() { return baseObject; }

  private long baseOffset;
  public long getBaseOffset() { return baseOffset; }

  private int numFields;
  public int numFields() { return numFields; }

  private int sizeInBytes;
  public int getSizeInBytes() { return sizeInBytes; }

  /** The width of the null tracking bit set, in bytes */
  private int bitSetWidthInBytes;

  private long getFieldOffset(int ordinal) {
    return baseOffset + bitSetWidthInBytes + ordinal * 8L;
  }

  private void assertIndexIsValid(int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    assert index < numFields : "index (" + index + ") should < " + numFields;
  }

  /**
   * Construct a new UnsafeGenericHandle. The data is not pointed yet
   * the value returned by this constructor is equivalent to a null pointer.
   */
/*  public UnsafeGenericHandle(int numFields) {
    this.numFields = numFields;
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
  }*/

  // for serializer
  public UnsafeGenericHandle() {
    this.numFields = 0;
    this.sizeInBytes = 0;
    this.baseObject = null;
    this.baseOffset = 0;
  }

  /**
   * Creates an empty UnsafeGenericHandle from a byte array with specified numFields.
   */
  public UnsafeGenericHandle(int numFields) {
    this.numFields = numFields;
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.sizeInBytes = (int)((numFields * 8L) + this.bitSetWidthInBytes);

    mem = manager.allocate(sizeInBytes);
    this.pointTo(mem.baseObject, mem.offSet, (int)mem.size);

    //long memAddr = UnsafeWrapper.allocateMemory(sizeInBytes);
    //this.pointTo(null, memAddr, sizeInBytes);
    
    //this.pointTo(new byte[sizeInBytes], sizeInBytes);
    //System.out.format("The required size is %d\n", sizeInBytes);
    //UnsafeWrapper.setMemory(object, UnsafeWrapper.BYTE_ARRAY_OFFSET, sizeInBytes, )
  }

  public UnsafeGenericHandle(int numFields, int size) {
    this.numFields = numFields;
    this.bitSetWidthInBytes = calculateBitSetWidthInBytes(numFields);
    this.sizeInBytes = (int)((size) + this.bitSetWidthInBytes);

    mem = manager.allocate(sizeInBytes);
    this.pointTo(mem.baseObject, mem.offSet, (int)mem.size);
    /*long memAddr = UnsafeWrapper.allocateMemory(sizeInBytes);
    this.pointTo(null, memAddr, sizeInBytes);*/
    
    //this.pointTo(new byte[sizeInBytes], sizeInBytes);
    //System.out.format("The required size is %d\n", sizeInBytes);
  }

  /**
   * duplicate the data, returns a handle with newly allocated underlying byte array
   */

  public UnsafeGenericHandle duplicate() {
    /* constructor of copy already allocates memory. Just copy */
    UnsafeGenericHandle copy = new UnsafeGenericHandle(numFields, sizeInBytes);
    UnsafeWrapper.copyMemory(baseObject, baseOffset,
                             copy.baseObject, copy.baseOffset, 
                             sizeInBytes);
    return copy;
  }

/*  public ArrayData getArray(int ordinal) {
    if (isNullAt(ordinal)) {
      return null;
    } else {
      final long offsetAndSize = getLong(ordinal);
      final int offset = (int) (offsetAndSize >> 32);
      final int size = (int) (offsetAndSize & ((1L << 32) - 1));
      return UnsafeReaders.readArray(baseObject, baseOffset + offset, size);
    }
  }*/

  public void setDoubleArr(int index, double[] arr) {
    int arr_offset = sizeInBytes;
    long arr_size = arr.length * 8L;
    /* assume arr length will not be larger than 2 ^ 32 - 1 */
    long encode_offset_and_size = (arr_offset << 32) + arr_size;
    UnsafeWrapper.putLong(baseObject, getFieldOffset(index), encode_offset_and_size);

    long final_length = sizeInBytes + arr_size;

    

    MemorySegment new_mem = manager.allocate(final_length);


    UnsafeWrapper.copyMemory(null, baseOffset, 
                             new_mem.baseObject, new_mem.offSet, 
                             sizeInBytes);
    UnsafeWrapper.copyMemory(arr, UnsafeWrapper.DOUBLE_ARRAY_OFFSET, 
                             new_mem.baseObject, new_mem.offSet + arr_offset,
                             arr_size);
    manager.free(mem);
    baseObject = new_mem.baseObject;
    sizeInBytes = (int)final_length;

    /*long new_mem = UnsafeWrapper.allocateMemory(final_length);
    UnsafeWrapper.freeMemory(baseOffset);
    baseOffset = new_mem;
    sizeInBytes = (int)final_length;*/
  }

  /**
   * Update this UnsafeGenericHandle to point to different backing data.
   */
  public void pointTo(Object baseObject, long baseOffset, int sizeInBytes) {
    assert numFields >= 0 : "numFields (" + numFields + ") should >= 0";
    this.baseObject = baseObject;
    this.baseOffset = baseOffset;
    this.sizeInBytes = sizeInBytes;
  }

  /**
   * Update this UnsafeGenericHandle to point to the underlying byte array.
   */
  public void pointTo(byte[] buf, int sizeInBytes) {
    pointTo(buf, UnsafeWrapper.BYTE_ARRAY_OFFSET, sizeInBytes);
  }

  public void free() {
    manager.free(mem);
    mem = null;
    /*if (baseObject == null) {
      UnsafeWrapper.freeMemory(baseOffset);
    } else {
      System.out.println("impossible object is not null");
    }*/
  }

  public void setTotalSize(int sizeInBytes) {
    this.sizeInBytes = sizeInBytes;
  }

  // public void BitSet.unset(baseObject, baseOffset, int i) {
  //   assertIndexIsValid(i);
  //   BitSet.unset(baseObject, baseOffset, i);
  // }

  public void eraseAt(int i) {
    assertIndexIsValid(i);
    BitSet.set(baseObject, baseOffset, i);
    // putLong will zero out the word
    UnsafeWrapper.putLong(baseObject, getFieldOffset(i), 0);
  }

/*  @Override
  public void update(int ordinal, Object value) {
    throw new UnsupportedOperationException();
  }*/

  public void setInt(int ordinal, int value) {
    //assertIndexIsValid(ordinal);
    //BitSet.unset(baseObject, baseOffset, ordinal);
    UnsafeWrapper.putInt(baseObject, getFieldOffset(ordinal), value);
  }

  public void setLong(int ordinal, long value) {
    //assertIndexIsValid(ordinal);
    //BitSet.unset(baseObject, baseOffset, ordinal);
    UnsafeWrapper.putLong(baseObject, getFieldOffset(ordinal), value);
  }

  public void setDouble(int ordinal, double value) {
    /*assertIndexIsValid(ordinal);
    BitSet.unset(baseObject, baseOffset, ordinal);
    if (Double.isNaN(value)) {
      value = Double.NaN;
    }*/
    UnsafeWrapper.putDouble(baseObject, getFieldOffset(ordinal), value);
  }

  public void setBoolean(int ordinal, boolean value) {
    //assertIndexIsValid(ordinal);
    //BitSet.unset(baseObject, baseOffset, ordinal);
    UnsafeWrapper.putBoolean(baseObject, getFieldOffset(ordinal), value);
  }

  public void setShort(int ordinal, short value) {
    //assertIndexIsValid(ordinal);
    //BitSet.unset(baseObject, baseOffset, ordinal);
    UnsafeWrapper.putShort(baseObject, getFieldOffset(ordinal), value);
  }

  public void setByte(int ordinal, byte value) {
    //assertIndexIsValid(ordinal);
    //BitSet.unset(baseObject, baseOffset, ordinal);
    UnsafeWrapper.putByte(baseObject, getFieldOffset(ordinal), value);
  }

  public void setFloat(int ordinal, float value) {
    /*assertIndexIsValid(ordinal);
    BitSet.unset(baseObject, baseOffset, ordinal);
    if (Float.isNaN(value)) {
      value = Float.NaN;
    }*/
    UnsafeWrapper.putFloat(baseObject, getFieldOffset(ordinal), value);
  }

  public boolean isNullAt(int ordinal) {
    assertIndexIsValid(ordinal);
    return BitSet.isSet(baseObject, baseOffset, ordinal);
  }

  public boolean getBoolean(int ordinal) {
    //assertIndexIsValid(ordinal);
    return UnsafeWrapper.getBoolean(baseObject, getFieldOffset(ordinal));
  }

  public byte getByte(int ordinal) {
    //assertIndexIsValid(ordinal);
    return UnsafeWrapper.getByte(baseObject, getFieldOffset(ordinal));
  }

  public short getShort(int ordinal) {
    //assertIndexIsValid(ordinal);
    return UnsafeWrapper.getShort(baseObject, getFieldOffset(ordinal));
  }

  public int getInt(int ordinal) {
    //assertIndexIsValid(ordinal);
    return UnsafeWrapper.getInt(baseObject, getFieldOffset(ordinal));
  }

  public long getLong(int ordinal) {
    //assertIndexIsValid(ordinal);
    return UnsafeWrapper.getLong(baseObject, getFieldOffset(ordinal));
  }

  public float getFloat(int ordinal) {
    //assertIndexIsValid(ordinal);
    return UnsafeWrapper.getFloat(baseObject, getFieldOffset(ordinal));
  }

  public double getDouble(int ordinal) {
    //assertIndexIsValid(ordinal);
    return UnsafeWrapper.getDouble(baseObject, getFieldOffset(ordinal));
  }


  /*public UnsafeGenericHandle duplicate() {
    UnsafeGenericHandle copy = new UnsafeGenericHandle(numFields);
    final byte[] dataCopy = new byte[sizeInBytes];
    UnsafeWrapper.copyMemory(baseObject, baseOffset,
                        dataCopy, UnsafeWrapper.BYTE_ARRAY_OFFSET,
                        sizeInBytes);
    copy.pointTo(dataCopy, UnsafeWrapper.BYTE_ARRAY_OFFSET, sizeInBytes);
    return copy;
  }*/


  /**
   * Copies the input UnsafeGenericHandle to this UnsafeGenericHandle, and resize the underlying byte[]
   */
  public void copyFrom(UnsafeGenericHandle src) {
    // copyFrom is only available for UnsafeGenericHandle created from byte array.
    assert (baseObject instanceof byte[]) && baseOffset == UnsafeWrapper.BYTE_ARRAY_OFFSET;
    if (src.sizeInBytes > this.sizeInBytes) {
      // resize the underlying byte[] if it's not large enough.
      this.baseObject = new byte[src.sizeInBytes];
    }
    UnsafeWrapper.copyMemory(src.baseObject, src.baseOffset, 
                        this.baseObject, this.baseOffset, 
                        src.sizeInBytes);
    this.sizeInBytes = src.sizeInBytes;
  }

  /**
   * Returns the underlying bytes for this UnsafeGenericHandle.
   */
  public byte[] getBytes() {
    if (baseObject instanceof byte[] && baseOffset == UnsafeWrapper.BYTE_ARRAY_OFFSET
      && (((byte[]) baseObject).length == sizeInBytes)) {
      return (byte[]) baseObject;
    } else {
      byte[] bytes = new byte[sizeInBytes];
      UnsafeWrapper.copyMemory(baseObject, baseOffset, bytes, UnsafeWrapper.BYTE_ARRAY_OFFSET, sizeInBytes);
      return bytes;
    }
  }

  // This is for debugging
  public String toString() {
    StringBuilder build = new StringBuilder("[");
    for (int i = 0; i < sizeInBytes; i += 8) {
      if (i != 0) build.append(',');
      build.append(java.lang.Long.toHexString(UnsafeWrapper.getLong(baseObject, baseOffset + i)));
    }
    build.append(']');
    return build.toString();
  }

  public static void main(String[] argv)
  {
    System.out.format("Printing 1st line \n");
  }

}
