package research;

import java.util.HashMap;
import java.util.BitSet;

public class MemoryPoolAllocator implements MemoryAllocator{
    private class MemoryBucket  {
        public long objectSize;
        public long poolSizeBytes;
        public long baseAddr;
        private java.util.BitSet freeMap;
        MemoryBucket (long _objectSize) {
            objectSize = _objectSize;
            poolSizeBytes = objectSize * 4096;
            baseAddr = UnsafeWrapper.allocateMemory(poolSizeBytes);
            freeMap = new java.util.BitSet(4096);
        }

        long allocate () {
            int index = freeMap.nextClearBit(0);
            if (index >= 4096) {
                return 0;
            } else {
                freeMap.set(index);
                return index;
            }
        }

        void free (long ptr) {
            long index = (ptr - baseAddr) / objectSize;
            freeMap.set((int)index, false);
        }
    }

    private HashMap<Long, MemoryBucket> buckets = new HashMap<>();
    private HashMap<Long, MemoryBucket> allocateMap = new HashMap<>();

    public MemorySegment allocate (long size) {
        for (MemoryBucket b : buckets.values()) {
            if (b.objectSize == size) {
                long ptr = b.allocate();
                if (ptr != 0) {
                    allocateMap.put(ptr, b);
                    return new MemorySegment(null, ptr, size);
                }
            }
        }
        MemoryBucket bucket = new MemoryBucket(size);
        buckets.put(bucket.baseAddr, bucket);
        long ptr = bucket.allocate();
        allocateMap.put(ptr, bucket);
        return new MemorySegment(null, ptr, size);
    }

    public void free (long ptr) {
        if (!allocateMap.containsKey(ptr)) {
            throw new RuntimeException("No entry in allocateMap");
        } 
        MemoryBucket b = allocateMap.get(ptr);
        allocateMap.remove(ptr);
        b.free(ptr);
    }

}