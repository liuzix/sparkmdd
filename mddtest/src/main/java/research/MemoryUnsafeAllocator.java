package research;


public class MemoryUnsafeAllocator implements MemoryAllocator {
    public long allocate (long size) {
        return UnsafeWrapper.allocateMemory(size);
    }

    public void free (long ptr) {
        return UnsafeWrapper.freeMemory(ptr);
    }
}