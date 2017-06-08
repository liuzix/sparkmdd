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

    public long allocate (long size) {
        for (MemoryBucket b : buckets.values()) {
            if (b.objectSize == size) {
                long ptr = b.allocate();
                if (ptr != 0) {
                    return ptr;
                }
            }
        }
        MemoryBucket bucket = new MemoryBucket(size);
        buckets.put(bucket.baseAddr, bucket);
        return bucket.allocate();
    }

    public void free (long ptr) {

    }

}
