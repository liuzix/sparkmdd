package research;

/*
Two situations: 1. We use JVM managed memory, then baseObject is not null
                2. We use unsafe memory, then baseObject is null, offSet is just the address we get from the allocator.

*/
public class MemorySegment {
    public Object baseObject;
    public long offSet;
    public long size;

    public MemorySegment (Object _o, long _offset, long _size) {
        baseObject = _o;
        offSet = _offset;
        size = _size;
    }
}