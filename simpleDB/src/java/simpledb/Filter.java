package simpledb;

import java.util.*;

//Filter 过滤器 是一个实现关系选择的运算符，它会过滤掉所有与谓词不匹配的元组。
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    
    private Predicate pred;
    private OpIterator child;
    
    public Filter(Predicate p, OpIterator child) {
        this.pred=p;
        this.child=child;
    }

    public Predicate getPredicate() {
       return this.pred;
    }

    public TupleDesc getTupleDesc() {
       return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
    	super.open();
        child.open();
    }

    public void close() {
    	super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException { 
        //这个函数的意思就是找Predicate.filter() 返回 true 的元组
    	Tuple temp=null;
    	while(child.hasNext()) {
    		temp=child.next();
    		if(pred.filter(temp))
    			return temp;
    	}   	
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] Children=new OpIterator[1];
        Children[0]=this.child;
        return Children;
     }

    @Override
    public void setChildren(OpIterator[] children) {
    	this.child=children[0];
    }

}
