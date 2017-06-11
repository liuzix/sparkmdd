package research;


public class MemoryGCAllocator implements MemoryAllocator {
    public MemorySegment allocate (long size) {
        byte[] bytes = new byte[(int)size + UnsafeWrapper.BYTE_ARRAY_OFFSET];
        return new MemorySegment(bytes, UnsafeWrapper.BYTE_ARRAY_OFFSET, size);
    }

    public void free (MemorySegment ms) {

    }
}