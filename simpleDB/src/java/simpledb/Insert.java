package simpledb;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator { //将从子运算符读取的元组插入构造函数中指定的 tableId
//是最顶层的运算符，返回受到影响的元组
//该运算符将从其子运算符读取的元组添加到指定的 tableid
//在它的构造函数中。 它应该使用 BufferPool.insertTuple()方法来执行此操作。
    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    private TransactionId t; //运行插入的事务
    private OpIterator child; //从中读取要插入的元组的子运算符
    private int tableId; //插入元组的表
    
    private TupleDesc td;
    private Tuple tuple;
    private boolean iscalled; //用于fetchNext函数判断
    
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
    	if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
    		throw new DbException("The TupleDesc is not matched");
    	this.t=t;
    	this.child=child;
    	this.tableId=tableId;
    	this.td=new TupleDesc(new Type[] {Type.INT_TYPE},new String[] {""});
    	this.tuple=new Tuple(td);
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
    	super.open();
    	child.open();
    }

    public void close() {
        super.close();
        child.close();
        iscalled=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    
    //将从 child 读取的元组插入到构造函数指定的 tableId 中。 
    //它返回一个包含插入记录数的单字段元组。 插入应该通过 BufferPool 传递。
    //BufferPool 的实例可通过 Database.getBufferPool() 获得。
    //请注意，insert 在插入之前不需要检查特定元组是否重复。
    //包含插入记录数的 1 字段元组，如果多次调用则为 null。
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(iscalled)
    		return null;
    	iscalled=true;
    	int num=0;
    	while(child.hasNext()) {
    		try {
				Database.getBufferPool().insertTuple(t, tableId, child.next());
				num++;
			} catch (NoSuchElementException | DbException | IOException | TransactionAbortedException e) {
				e.printStackTrace();
			}
    	}
    	tuple.setField(0,new IntField(num));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
    	OpIterator[] children=new OpIterator[1];
    	children[0]=child;
    	return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child=children[0];
    }
}
