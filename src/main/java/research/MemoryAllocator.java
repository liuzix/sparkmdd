package research;

public interface MemoryAllocator {
    MemorySegment allocate (long size);
    void free (MemorySegment ms);
}
