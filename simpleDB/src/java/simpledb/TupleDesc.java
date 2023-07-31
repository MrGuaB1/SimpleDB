package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable { //元组的schema

	
	private ArrayList<TDItem> items=new ArrayList<TDItem>(); //Iterator<TDItem> iterator()
	
    /**
     * A help class to facilitate organizing the information of each field 
     * */
    public static class TDItem implements Serializable { //方便整理各个字段的信息，如Age(Type.int,19)
    
        private static final long serialVersionUID = 1L; //序列化，Serializable 
        /**
         * The type of the field
         * */
        public final Type fieldType; //字段类型，Type是一个枚举，simpleDB中只有int和String两种类型
        
        /**
         * The name of the field
         * */
        public final String fieldName; //字段名字

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() { 
            return fieldName + "(" + fieldType + ")";
        }
    }
//-------------------------辅助类结束-------------------------------------
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        Iterator<TDItem> it=items.iterator();
        return it;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) { //构造函数
        for(int i=0;i<typeAr.length;i++)
        	items.add(new TDItem(typeAr[i],fieldAr[i]));
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        for(int i=0;i<typeAr.length;i++)
        	items.add(new TDItem(typeAr[i],null)); //未提供field，传空值
    }
    public TupleDesc() {} 

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
       return items.size();
    }
    
    public ArrayList<TDItem> getItems(){
    	return this.items;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
       if(i<0||i>=this.numFields())
    	   throw new NoSuchElementException();
       return items.get(i).fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if(i<0||i>=this.numFields())
     	   throw new NoSuchElementException();
    	return items.get(i).fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {//这就是一个查找函数，遍历即可
        if(name==null)
        	throw new NoSuchElementException();
        for(int i=0;i<items.size();i++) {
        	if(name.equals(items.get(i).fieldName))
        		return i;
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() { //注意这是以字节为单位的
        int bytes=0;
        for(int i=0;i<items.size();i++)
        	bytes+=this.getFieldType(i).getLen(); //Type类中定义的方法，获取占用的内存，int为4，字符串为128
        return bytes;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) { //合并函数
        TupleDesc td3=new TupleDesc(); //在上面添加了一个默认构造函数
        for(int i=0;i<td1.items.size();i++)
        	td3.items.add(td1.items.get(i));
        for(int i=0;i<td2.items.size();i++)
        	td3.items.add(td2.items.get(i));       
        return td3;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) { //比较值相等的函数，注意这是Object
        if(o==null)
        	return false; //空对象返回False
        if(o.getClass().equals(TupleDesc.class)) {
        	TupleDesc temp=(TupleDesc) o; //类型转换
        	if(this.items.size()!=temp.items.size())
        		return false;
        	for(int i=0;i<this.items.size();i++) //
        		if(!(items.get(i).fieldType.equals(temp.items.get(i).fieldType)))
        			return false;  //类型不同名字肯定不同，无需再比较了
        	return true;
        }
        return false; //类型不对直接返回False；
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
       StringBuffer str=new StringBuffer();
       for(int i=0;i<this.items.size()-1;i++) 
    	   str.append(this.items.get(i).toString()+","); //在静态内部类中已经替我们实现好了这种toString格式
       str.append(this.items.get(items.size()-1));
       return str.toString();
    }
}
