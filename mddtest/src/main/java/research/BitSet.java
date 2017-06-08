package research;


/**
 * Utility class with only static methods
 * A bit set occupies multiples of 8 bytes in memory
 */

/*
 * Bit shifting logic:
 * Given the index i.e. 130, extract the low bits correponding to mod 64,
 * and then extract the correponding word from the object
 * operate on the word with the mod 64 low bits, and then put back 
 */
public final class BitSet {

  private static final long WORD_SIZE = 8;

  private BitSet() {}

  /**
   * Sets the bit at the specified index to True
   */
  public static void set(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;
    final long word = UnsafeWrapper.getLong(baseObject, wordOffset);
    UnsafeWrapper.putLong(baseObject, wordOffset, word | mask);
  }

  /**
   * Sets the bit at the specified index to False.
   */
  public static void unset(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;
    final long word = UnsafeWrapper.getLong(baseObject, wordOffset);
    UnsafeWrapper.putLong(baseObject, wordOffset, word & ~mask);
  }

  /**
   * Returns true if the bit is set at the specified index.
   */
  public static boolean isSet(Object baseObject, long baseOffset, int index) {
    assert index >= 0 : "index (" + index + ") should >= 0";
    final long mask = 1L << (index & 0x3f);  // mod 64 and shift
    final long wordOffset = baseOffset + (index >> 6) * WORD_SIZE;
    final long word = UnsafeWrapper.getLong(baseObject, wordOffset);
    return (word & mask) != 0;
  }

  /**
   * Returns true if any bit is set.
   */
  public static boolean anySet(Object baseObject, long baseOffset, long bitSetWidthInWords) {
    long addr = baseOffset;
    for (int i = 0; i < bitSetWidthInWords; i++, addr += WORD_SIZE) {
      if (UnsafeWrapper.getLong(baseObject, addr) != 0) {
        return true;
      }
    }
    return false;
  }
}
