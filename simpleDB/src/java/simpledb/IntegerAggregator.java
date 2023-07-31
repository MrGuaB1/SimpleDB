package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    
    private int gbfield; //元组中分组字段的从 0 开始的索引，如果没有分组则为 NO_GROUPING(静态的整形变量)
    private int afield; //元组中聚合字段的从 0 开始的索引
    private Type gbfieldtype; //按字段分组的类型（例如 Type.INT_TYPE），如果没有分组则为 null
    private Op what; //使用什么聚合运算符
    
    private ArrayList<Tuple> tupleList;
    private Map<Field,Integer> groupPairMap; //用于计算除avg外的聚合函数的映射
    private Map<Field,ArrayList<Integer>> avg; //用于计算列均值

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
    	this.what=what;
    	this.afield=afield;
    	this.gbfield=gbfield;
    	this.gbfieldtype=gbfieldtype;
    	
    	this.tupleList=new ArrayList<Tuple>();
    	this.groupPairMap=new HashMap<>(); 
    	this.avg=new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     *            包含聚合字段和分组字段的 Tuple
     */
    
    //将一个新的元组合并到聚合中，按照构造函数中的指示进行分组
    //有五个聚合函数：sum：列的和 avg：列均值 max和min count:行的数目
    
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupValue =this.gbfield==NO_GROUPING?null:tup.getField(gbfield); //gbField的要求
    	int aggregateValue=Integer.parseInt(tup.getField(afield).toString()); //聚合值
    	switch(what) {
    		case SUM:
    			int sum=aggregateValue;
    			for(Map.Entry<Field, Integer> entry:this.groupPairMap.entrySet()) {
    				if(entry.getKey().equals(groupValue))
    					sum+=entry.getValue();
    			}
    			this.groupPairMap.put(groupValue, sum); //映射是互异的，直接put没有问题
    			break;
    		case MIN:
    			int min=aggregateValue;
    			for(Map.Entry<Field, Integer> entry:this.groupPairMap.entrySet()) {
    				if(entry.getKey().equals(groupValue)) {
    					if(min>entry.getValue())
    						min=entry.getValue();
    				}
    			}
    			this.groupPairMap.put(groupValue, min); 
    			break;
    		case MAX:
    			int max=aggregateValue;
    			for(Map.Entry<Field, Integer> entry:this.groupPairMap.entrySet()) {
    				if(entry.getKey().equals(groupValue)) {
    					if(max<entry.getValue())
    						max=entry.getValue();
    				}
    			}
    			this.groupPairMap.put(groupValue, max); 
    			break;
    		case COUNT:
    			if(!this.groupPairMap.containsKey(groupValue)) 
    				this.groupPairMap.put(groupValue, 1);
    			else
    				this.groupPairMap.put(groupValue,this.groupPairMap.get(groupValue)+1);
    			break;
    		case AVG:
    			if(!avg.containsKey(groupValue)) {
    				avg.put(groupValue, new ArrayList<>());
    				avg.get(groupValue).add(aggregateValue);
    			}
    			else
    				avg.get(groupValue).add(aggregateValue);
    			ArrayList<Integer> sum_for_avg=avg.get(groupValue);
    			int temp=0;
    			for(int i=0;i<sum_for_avg.size();i++)
    				temp+=sum_for_avg.get(i);
    			this.groupPairMap.put(groupValue, temp/sum_for_avg.size());
    			break;   				
    	}
    	
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 在组聚合结果上创建一个 OpIterator
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     *         
     *         一个 OpIterator，如果使用组，其元组为对 (groupVal, aggregateVal)
     *         如果没有分组，则为单个 (aggregateVal)。 aggregateVal 由构造函数中指定的聚合类型确定。
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
