package research;

import research.MemoryAllocator;

public class MemoryGCAllocator implements MemoryAllocator {
    public MemorySegment allocate (long size) {
        Byte[] bytes = new Byte[(int)size];
        return new MemorySegment(bytes, 0, size);
    }

    public void free (MemorySegment ms) {

    }
}