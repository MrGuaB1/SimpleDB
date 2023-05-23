package simpledb;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 */
public class Predicate implements Serializable { //谓词将元组与指定的字段值进行比较。

    private static final long serialVersionUID = 1L;

    /** Constants used for return codes in Field.compare */
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;
    	//=  >  <  >=  <=  Like !=（<>）
        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "LIKE";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }

    }
    
    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     * @param op
     *            operation to use for comparison
     * @param operand
     *            field value to compare passed in tuples to
     */
    private int field;
    private Op op;
    private Field operand;
    
    public Predicate(int field, Op op, Field operand) {
        this.op=op;
        this.field=field;
        this.operand=operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        return this.field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        return this.op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
       return this.operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    //使用构造函数中特定的运算符将构造函数中指定的 t 的field number与构造函数中指定的操作数field number进行比较
    //可以通过 Field 的 compare 方法进行比较。
    public boolean filter(Tuple t) {
       return t.getField(field).compare(op, operand); //将操作数operand使用运算op进行过滤
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string"
     */
    public String toString() {
       return "f="+field+" op="+op.toString()+" operand="+operand.toString();
    }
}
