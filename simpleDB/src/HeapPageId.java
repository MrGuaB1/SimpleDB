package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
	private int tableId; //被引用的表的编号
	private int pgNo; //页的编号
    public HeapPageId(int tableId, int pgNo) {
        this.tableId=tableId;
        this.pgNo=pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
       return this.tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
       return this.pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() { //此页面的哈希码，由tableId和pgNo表示
        //使用场景如：如果将 PageId 用作 BufferPool 中的哈希表中的键
    	//Database是一个单例，为了保证数据的唯一性，应通过Database来获取线程池
        return Database.getBufferPool().hashCode(); 
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        if(o==null)
        	return false;
        if(!(o.getClass().equals(HeapPageId.class)))
        	return false;
        PageId temp=(PageId) o;
        if(this.tableId==temp.getTableId()&&this.pgNo==temp.getPageNumber())
        	return true;
        return false;
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = getPageNumber();

        return data;
    }

}
