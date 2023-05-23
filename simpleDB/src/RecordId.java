package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable { //RecordId 是对特定表的特定页上的特定元组的引用。

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    private PageId pid;
    private int tupleno;
    public RecordId(PageId pid, int tupleno) {
       this.pid=pid;
       this.tupleno=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        return this.tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
       return this.pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        if(o==null)
        	return false;
        if(!o.getClass().equals(RecordId.class))
        	return false;
        RecordId temp=(RecordId) o;
        if(this.tupleno==temp.getTupleNumber()&&this.pid.equals(temp.getPageId()))
        	return true;
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 两个相等的RecordID应有相同的hash码
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
    	//判断相等的条件是比较PageId的pid和int型的tupleNo
    	return this.tupleno+this.pid.getPageNumber()+this.pid.getTableId();
    }

}
