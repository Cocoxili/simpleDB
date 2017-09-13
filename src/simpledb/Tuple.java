package simpledb;

import java.util.NoSuchElementException;

/**
 * Tuple maintains information about the contents of a tuple.
 * Tuples have a specified schema specified by a TupleDesc object and contain
 * Field objects with the data for each field.
 */
public class Tuple {
	
	private RecordId recordId;
	private int num;
	private Field [] field;	
	private int [] attr;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     * instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
    	num = td.numFields();
    	field = new Field[num];
    	attr = new int[num];
    	//attr[i] = 0 means field[i] is int; attr[i] = 1 means field[i] is string.
    	for(int i = 0; i < num; i++){
    		if(td.fieldType[i] == Type.INT_TYPE)
    			attr[i] = 0;
    		else if(td.fieldType[i] == Type.STRING_TYPE)
    			attr[i] = 1;
    	}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type [] ty = new Type [num];
    	for(int i = 0; i < num; i++){
    		if(attr[i] == 0)
    			ty[i] = Type.INT_TYPE;
    		else if(attr[i] == 1)
    			ty[i] = Type.STRING_TYPE;
    	}
    	
    	TupleDesc td = new TupleDesc(ty);
    	
    	for(int i = 0; i < num; i++){
    		td.fieldName[i] = td.getFieldName(i);
    	}
        return td;
    }

    /**
     * @return The RecordId representing the location of this tuple on
     *   disk. May be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
    	recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
    	if(i < 0 || i > num)
    		throw new NoSuchElementException("No field found.\n");
    	
    	if(f instanceof IntField){// f is intfield 			
    		IntField obj = (IntField)f;
        	field[i] = new IntField(obj.getValue());//IntField implements Field   
    	}
    	
    	if(f instanceof StringField){ //f is StringField
    		StringField obj = (StringField)f;
        	field[i] = new StringField(obj.getValue(), 1024);
    		   		
    	} 		
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
    	if( i < 0 || i > num)
    		return null;
        return field[i];
    }

    /**
     * Returns the contents of this Tuple as a string.
     * Note that to pass the system tests, the format needs to be as
     * follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     *
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        // some code goes here
    	int itx = 0;
    	while(itx < num){
    		System.out.println(getField(itx).toString()+"\t");
    	}
    	//System.out.println("\n");
        throw new UnsupportedOperationException("Implement this");
    }
    
    /**
     * used for class join.
     * */
    public static Tuple tuplesCombine(Tuple t1, Tuple t2){
    	TupleDesc td = TupleDesc.combine(t1.getTupleDesc(), t2.getTupleDesc());
    	Tuple tp = new Tuple(td);
    	int i = 0;
    	int j = 0;
    	for(; i < t1.getTupleDesc().numFields(); i++){
    		tp.setField(i, t1.getField(i));
    	}
    	for(; j < t2.getTupleDesc().numFields();  j++){
    		tp.setField(i+j, t2.getField(j));
    	}
    	return tp;
    }
}
