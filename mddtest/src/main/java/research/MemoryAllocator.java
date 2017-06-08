package research;

public interface MemoryAllocator {
    long allocate (long size);
    void free (long ptr);
}
