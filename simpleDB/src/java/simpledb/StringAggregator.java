package simpledb;

import simpledb.Aggregator.Op;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private int gbfield; //元组中分组字段的从 0 开始的索引，如果没有分组则为 NO_GROUPING(静态的整形变量)
    private int afield; //元组中聚合字段的从 0 开始的索引
    private Type gbfieldtype; 
    private Op what; 
    private Map<Field,Integer> groupPairMap;
    private ArrayList<Tuple> tupleList;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	this.afield=afield;
    	this.what=what;
    	this.groupPairMap=new HashMap<>();
    	this.tupleList=new ArrayList<Tuple>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    
    //字符串聚合只有count
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field groupValue=this.gbfield==NO_GROUPING?null:tup.getField(this.gbfield);
    	switch(what) {
    		case COUNT:
    			if(!this.groupPairMap.containsKey(groupValue)) 
    				this.groupPairMap.put(groupValue, 1);
    			else
    				this.groupPairMap.put(groupValue, this.groupPairMap.get(groupValue)+1);
    			break; 			
    		default:break;
    	}
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	//先描述两个元组模式：
    	TupleDesc td1 = new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},
                new String[]{"groupVal","aggregateVal"});
        TupleDesc td2 = new TupleDesc(new Type[]{Type.INT_TYPE},
                new String[]{"aggregateVal"});
    	
    	//根据两种描述方式，实现不同的方法：
    	for(Map.Entry<Field, Integer> entry:this.groupPairMap.entrySet()) {
    		if(entry.getKey()!=null) { //td1模式
    			Tuple tuple=new Tuple(td1);
    			tuple.setField(0,entry.getKey()); //Tuple中的设置字段方法，在数组中加Field
    			tuple.setField(1,new IntField(entry.getValue())); //IntField为存储单个整数的 Field 实例
    			this.tupleList.add(tuple);
    		}
    		else {
    			Tuple tuple=new Tuple(td2);
    			tuple.setField(0,entry.getKey());
    			this.tupleList.add(tuple);
    		}
    	}
    	
    	//不能直接返回tupleList.iterator()，类型不对，要继承之后向上转型
    	return new OpIterator() {
			
    		private boolean isOpen;
    		private Iterator<Tuple> it;
    		
			@Override
			public void rewind() throws DbException, TransactionAbortedException {
				close();
				open();
			}
			
			@Override
			public void open() throws DbException, TransactionAbortedException {
				isOpen=true;
				it=tupleList.iterator();
			}
			
			@Override
			public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
				if(!isOpen)
					throw new DbException("Closed");
				return it.next();
			}
			
			@Override
			public boolean hasNext() throws DbException, TransactionAbortedException {
				return isOpen&&it.hasNext();
			}
			
			@Override
			public TupleDesc getTupleDesc() {
				return tupleList.get(0).getTupleDesc();
			}
			
			@Override
			public void close() {
				isOpen=false;			
				it=null;
			}
		};
    }

}
