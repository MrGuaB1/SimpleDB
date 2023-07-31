package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
//SeqScan是一种顺序扫描访问方法的实现，它以无特定顺序（例如，当它们在磁盘上布局时）读取表的每个元组
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    //新增：
    private int tableid;
    private TransactionId tid;
    private String tableAlias;
    private DbFileIterator it; //SimpleDB每个运算符都实现 DbFileIterator接口
    
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    //此表的别名（解析器需要）； 返回的 tupleDesc 应该有名称为 tableAlias.fieldName 的字段
    //（注意：此类不负责处理 tableAlias 或 fieldName 为空的情况
    //如果它们是空的，它不应该崩溃，但结果名称可以是 null.fieldName,tableAlias .null 或 null.null）
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
    	this.tableAlias=tableAlias;
    	this.tid=tid;
    	this.tableid=tableid;
    	this.it=Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
    	return Database.getCatalog().getTableName(tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias(){
    	return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid=tableid;
        this.tableAlias=tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException { //open就是打开迭代器   
		it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    
    //返回带有来自底层 HeapFile 的字段名称的 TupleDesc，前缀为构造函数中的 tableAlias 字符串。
    //当连接包含同名字段的表时，此前缀很有用。
    //别名和名称应该用“.”分隔。 字符（例如，“alias.fieldName”）。
    public TupleDesc getTupleDesc() {
    	TupleDesc oldtTupleDesc = Database.getCatalog().getTupleDesc(tableid);
		Type[] fieldTypes = new Type[oldtTupleDesc.numFields()];
		String[] fieldWithPrefix = new String[oldtTupleDesc.numFields()];
		for (int i = 0; i < fieldWithPrefix.length; i++) {
			fieldTypes[i] = oldtTupleDesc.getFieldType(i);
			String prefix = (tableAlias == null || tableAlias == "") ? "null" : tableAlias;
			String oldfieldName = oldtTupleDesc.getFieldName(i);
			String fieldName = (oldfieldName == null || oldfieldName == "") ? "null" : oldtTupleDesc.getFieldName(i);
			fieldWithPrefix[i] = prefix + "." + fieldName;
		}
		return new TupleDesc(fieldTypes, fieldWithPrefix);
	}

    public boolean hasNext() throws TransactionAbortedException, DbException {     
		return it.hasNext();		
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
		return it.next();
    }

    public void close() {
    	it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        it.rewind();
    }
}
