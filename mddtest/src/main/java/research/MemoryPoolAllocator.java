package research;

import java.util.HashMap;
import java.util.BitSet;
import java.util.Vector;
import java.lang.Math;

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
                return index * objectSize + baseAddr;
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

    public void free (MemorySegment ms) {
        long ptr = ms.offSet;
        if (!allocateMap.containsKey(ptr)) {
            throw new RuntimeException("No entry in allocateMap");
        } 
        MemoryBucket b = allocateMap.get(ptr);
        allocateMap.remove(ptr);
        b.free(ptr);
    }

    public static void main (String[] s) {
        Vector<MemorySegment> list = new Vector<>();
        MemoryAllocator alloc = new MemoryPoolAllocator();
        for (int i = 0; i < 5; i++) {
            long size = (long)Math.pow(2, i);
            for (int j = 0; j < 1000; j++) {
                list.add(alloc.allocate(size));
            }
        }

        for (MemorySegment ms : list) {
            alloc.free(ms);
        }
    }

}
