package simpledb;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {
	
	int maxLen = 1024;
	int [] fieldID = new int [maxLen];
	String [] fieldName = new String [maxLen];
	int [] fieldSize = new int [maxLen];
	Type [] fieldType = new Type [maxLen];
	int num;

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields
     * fields, with the first td1.numFields coming from td1 and the remaining
     * from td2.
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc combine(TupleDesc td1, TupleDesc td2) {
        // some code goes here
    	int i, j, num1, num2, num3;
    	num1 = td1.num;
    	num2 = td2.num; 
    	num3 = num1 + num2;
    	
    	Type [] fieldType = new Type[num3];
    	String [] fieldName = new String[num3];
    	
    	// creat a new TupleDesc instance td.
    	for(i = 0; i < num1; i++)
    		fieldType[i] = td1.fieldType[i];
    	for(; i < num3; i++)
    		fieldType[i] = td2.fieldType[i-num1];
    	
    	for(j = 0; j < num1; j++)
    		fieldName[j] = td1.fieldName[j];
    	for(; j < num3; j++)
    		fieldName[j] = td2.fieldName[j-num1];
    	
    	TupleDesc td = new TupleDesc(fieldType, fieldName);
    	
    	// initialize td.
    	td.num = num3;
    	int k;
    	for(k = 0; k < num1; k++){
    		td.fieldID[k] = k;
    		td.fieldSize[k] = td1.fieldSize[k];
    	}
    	for(; k < num3; k++){
    		td.fieldID[k] = k;
    		td.fieldSize[k] = td2.fieldSize[k-num1];
    	}
    	
        return td;
    }

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
    	num = typeAr.length;
    	int i;
    	for(i = 0; i < num; i++){
    		fieldID[i] = i;
    		fieldType[i] = typeAr[i];
    		fieldName[i] = fieldAr[i];
    		fieldSize[i] = typeAr[i].getLen();
    	}
    }

    /**
     * Constructor.
     * Create a new tuple desc with typeAr.length fields with fields of the
     * specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
    	num = typeAr.length;
    	int i;
    	for(i = 0; i < num; i++){
    		fieldID[i] = i;
    		fieldType[i] = typeAr[i];
    		// fieldName[i] = NULL;
    		fieldSize[i] = typeAr[i].getLen();
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return num;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
    	if(i < 0 || i > num)
    		throw new NoSuchElementException("Invalid index.\n");
    	String name = fieldName[i];
        return name;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        // some code goes here
    	int i, j;
    	int nullNameCnt = 0;
    	//number of the field which names are null.
    	for(j = 0; j < num; j++)
    		if(fieldName[j] == null)
    			nullNameCnt ++;
    	
    	for(i = 0; i < num; i++)
    		if(name == null)
    			throw new NoSuchElementException("No a valid name.\n");
    		else if(nullNameCnt == num)//Is all field names all null?
    			throw new NoSuchElementException("No fields are named, so you can't find it\n");
    		else if(fieldName[i].compareTo(name) == 0)
    			return fieldID[i];
    	
    	if(i == num)
    		throw new NoSuchElementException("No field found.\n");
    	return -1;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        // some code goes here
    	if(i < 0 || i > num)
    		throw new NoSuchElementException("Not a valid index.");
    	else
    		return fieldType[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
    	int i = 0, size = 0;
    	for(; i < num; i++){
    		size += fieldSize[i];
    	}
        return size;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        // some code goes here
    	if(o == null)
    		return false;
    	if(o instanceof TupleDesc){
    		TupleDesc obj = (TupleDesc)o;
    		if(num != obj.num)
    			return false;
    		for(int i = 0; i < num; i++)
    			if(fieldName[i] != obj.fieldName[i])
    				return false;
    			else if(fieldType[i] != obj.fieldType[i])
    				return false;
    			else if(fieldSize[i] != obj.fieldSize[i])
    				return false;
    		return true;
    	}   	
    	
        return false;
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
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        return "";
    }
}
