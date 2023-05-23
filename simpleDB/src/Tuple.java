package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
//元组，包含由 TupleDesc 对象指定的指定模式 并包含 Field 对象每个字段的数据

//Serializable类是Java语言中的一个接口，用于表示一个类的对象可以被序列化为一个字节序列并在网络上传输或存储到磁盘上
//Serializable接口没有方法，只是标识这个类可以被序列化
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc td; //指定的模式
    ArrayList<Field> data=new ArrayList<Field>(); //Field对象每个字段的数据
    private RecordId rid; 

    /**
     * Create a new tuple with the specified schema (type). 即td
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.td=td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.rid;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	this.rid=rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) { //设置字段方法，i为索引，Field为内容
        // some code goes here
    	data.add(i,f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return (Field) data.get(i);
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
    	StringBuffer str=new StringBuffer(); //StringBuffer虽然慢，但是线程安全的
    	for(int i=0;i<data.size()-1;i++) {
    		str.append(data.get(i).toString()+"\t");
    	}
    	str.append(data.get(data.size()-1)); //保持格式
    	return str.toString();
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() //返回字段数据的迭代器
    {
        Iterator<Field> it=data.iterator();
        return it;
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
    	this.td=td;
    }
    
    public int data_size() {
    	return this.data.size();
    }
}
