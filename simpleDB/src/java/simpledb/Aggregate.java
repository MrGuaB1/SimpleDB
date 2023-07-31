package simpledb;
import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 * 计算聚合的聚合运算符（例如，sum、avg、max、min），请注意，我们仅支持单个列的聚合，按单个列分组。
 * SQL语句通过group by子句进行聚合
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 实施提示：根据 afield 的类型，
     * 您需要构造一个 {@link IntegerAggregator} 或 {@link StringAggregator} 来帮助您实施 readNext()。
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    
    private OpIterator child; //为我们提供元组的 OpIterator
    private int afield;  //表示在哪个列进行聚合
    private int gfield; //表示是否需要按照某个列进行分组。如果 gfield 的值为 -1，则表示不需要进行分组操作
    private Aggregator.Op aop; //要使用的聚合运算符(sum,avg,max,min)
    
    private Aggregator aggregator; //IntegerAggregator或StringAggregator
    private OpIterator it; //聚合器的迭代器
    private TupleDesc td; //元组的schema
    
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
    	this.aop=aop;
    	this.child=child;
    	this.afield=afield;
    	this.gfield=gfield;
    	
    	Type gfieldType=this.gfield==-1?null:this.child.getTupleDesc().getFieldType(gfield);
    	
    	//判断是int还是String，构造不同的聚合器：
    	if(this.child.getTupleDesc().getFieldType(afield)==Type.INT_TYPE)
    		this.aggregator=new IntegerAggregator(gfield,gfieldType,afield,aop);
    	else
    		this.aggregator=new StringAggregator(gfield,gfieldType,afield,aop);
    	this.it=this.aggregator.iterator();
    	
    	//按照两个名字函数的要求构造一个TupleDesc，不至于写很多判断条件：
    	ArrayList<Type> types=new ArrayList<Type>();
    	ArrayList<String> names=new ArrayList<String>();
    	if(gfieldType!=null) {
    		types.add(gfieldType);
    		names.add(this.child.getTupleDesc().getFieldName(gfield));
    	}
    	types.add(this.child.getTupleDesc().getFieldType(afield));
    	names.add(this.child.getTupleDesc().getFieldName(afield));
    	this.td=new TupleDesc(types.toArray(new Type[types.size()]),names.toArray(new String[names.size()]));
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     *         如果此聚合伴随有 groupby，则返回 INPUT 元组中的 groupby 字段索引。 
     *         如果不是，返回 {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
    	return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    
    //如果此聚合伴随有 groupby，则返回 OUTPUT 元组中 groupby字段的名称。如果不是，返回null
    public String groupFieldName() {
    	return this.td.getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() { //返回 OUTPUT 元组中聚合字段的名称
    	if(this.gfield==-1)
    		return this.td.getFieldName(0);
    	return this.td.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
    	return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException { 	
    	super.open();
    	this.child.open();
    	while(this.child.hasNext()) //把所有元组合并到聚合中
    		this.aggregator.mergeTupleIntoGroup(this.child.next());
    	it=this.aggregator.iterator();
    	it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     * 
     * 返回下一个元组。 如果有按字段分组，那么第一个字段就是我们分组的字段，第二个字段就是聚合计算的结果。
     * 如果没有按字段分组，则结果元组应包含一个表示聚合结果的字段。 如果没有更多的元组，应该返回 null。
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if(!it.hasNext())
    		return null;
    	return it.next();
    } 

    public void rewind() throws DbException, TransactionAbortedException {
		child.rewind();
		it.open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	return child.getTupleDesc();
    }

    public void close() {
		super.close();
		it.close();
		child.close();
    }

    @Override
    public OpIterator[] getChildren() {
    	OpIterator[] children=new OpIterator[1];
    	children[0]=this.child;
    	return children;
    }

    @Override
    public void setChildren(OpIterator[] children) {
    	this.child=children[0];
    }
    
}
