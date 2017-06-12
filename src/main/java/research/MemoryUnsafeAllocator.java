package research;


public class MemoryUnsafeAllocator implements MemoryAllocator {
    public MemorySegment allocate (long size) {
        return new MemorySegment(null, UnsafeWrapper.allocateMemory(size), size);
    }

    public void free (MemorySegment ms) {
        UnsafeWrapper.freeMemory(ms.offSet);
    }
}