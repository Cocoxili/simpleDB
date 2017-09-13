/** 
 public static int maxLen = 1024;
 test:TupleDesc actual = Database.getCatalog().getTupleDesc(-1); why -1?
 Table name shouldn't be foo.?
 --In TupleDescTest.java, Make sure you throw exception for non-existent fields
 接口函数如何实现，tuple.java中不用attr[]数组能否实现tuple初始化？
public synchronized Page getPage 

tableAlias the alias of this table (needed by the parser);
     *         the returned tupleDesc should have fields with name tableAlias.fieldName
**/
package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {
	
	public static int maxLen = 1024;

	int [] tableID = new int [maxLen];
	String [] tableName = new String [maxLen];
	TupleDesc [] tableTD = new TupleDesc [maxLen];
	DbFile [] tableFile = new DbFile [maxLen];
	int [] isTable = new int [maxLen];//Is table in the Database？
	
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
        // some code goes here
    	for(int i = 0; i < maxLen; i++){
    		isTable[i] = 0;
    	}
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name) {
        // some code goes here
    	int p = -1, i = 0;
    	if(name == null)
    		throw new NoSuchElementException("Table name shouldn't be NULL.\n");
    	for(i = 0; i < maxLen; i++)
    		if(isTable[i] == 0)
    		{//find an empty catelog item.
    			p = i;
    			tableID[p] = file.getId();
    			tableName[p] = name;
    			tableTD[p] = file.getTupleDesc();
    			tableFile[p] = file;
    			isTable[p] = 1;	 
    			//System.out.printf("Add table No.%d.\n", p);
    			break;
    		}
    	if(i == maxLen)
    		System.out.print("Catelog is full, cannot add table any more.\n");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param t the format of tuples that are being added
     */
    /*public void addTable(DbFile file) {
        addTable(file, "");
    }*/

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) {
        // some code goes here
    	int cur = 0;
    	if(name == null) throw new NoSuchElementException("null");
    	else{
    		for(int i = 0; i < maxLen; i++){
    			cur ++;
    			if(isTable[i] == 1)
    				if(tableName[i].equals(name))
    					return tableID[i];
    		}
    		if(cur == maxLen)
    			throw new NoSuchElementException("null");
    	}
    	return 0;
    	/*
    	int i;
    	for(i = 0; i < maxLen; i++){
    		
    		if(name == null)
    			throw new NoSuchElementException("Table name shouldn't be NULL.\n");
    		
    		else if(isTable[i] == 1)//(1)
    			if(tableName[i].equals(name))
    			//if(tableName[i].compareTo(name) == 0)//(2).（1）、（2）两个if不能更换位置，不然会出现null.compare(),会出错。
    				return tableID[i];  
    	}
    	if(i == maxLen)
    		throw new NoSuchElementException("Table doesn't exist.\n");
        return 0;
        */
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
        // some code goes here
    	int i = 0;
    	for(; i < maxLen; i++){
    		if(tableID[i] == tableid && isTable[i] == 1)
    				return tableTD[i];
    	}
    	if(i == maxLen)
    	{
    		throw new NoSuchElementException("Table doesn't exist./n");
    	}
    	return null;
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
        // some code goes here
    	int i;
    	for(i = 0; i < maxLen; i++)
    		if(tableID[i] == tableid && isTable[i] == 1)
    			return tableFile[i];
    	
    	if(i == maxLen)
    		throw new NoSuchElementException("Table doesn't exist./n");
        return null;
    }

    /** Delete all tables from the catalog */
    public void clear() {
    	// some code goes here
    	for(int i = 0; i < maxLen; i++){
    		isTable[i] = 0;
    	}
    	//System.out.print("clear finished.\n");
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(name + ".dat"), t);
                addTable(tabHf,name);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}
