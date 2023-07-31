package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private TransactionId t;
    private OpIterator child;
    private int num;
    private TupleDesc td;
    private Tuple tuple;
    private boolean called;
    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     *
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        this.t = t;
        this.child = child;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"number"});
        this.tuple = new Tuple(td);
        this.called = false;
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        super.close();
        child.close();
        called = false;
        num = 0;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        called = false;
        num = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     *
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(called)return null;
        called = true;
        while(child.hasNext()){
            try {
                Tuple tuple1 = child.next();
                Database.getBufferPool().deleteTuple(t,tuple1);
                num ++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tuple.setField(0,new IntField(num));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        OpIterator[] children = new OpIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }

}