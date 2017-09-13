package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {
	
	private TransactionId tid;
	private DbIterator dbIt;
	private int tableId;
	private boolean calledNext = false;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        // some code goes here
    	this.tid = t;
    	this.dbIt = child;
    	this.tableId = tableid;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type [] ty = new Type[1];
    	ty[0] = Type.INT_TYPE;
        return new TupleDesc(ty);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	dbIt.open();
    }

    public void close() {
        // some code goes here
    	dbIt.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	dbIt.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        // some code goes here
    	
    	//return null if called this method more than once
    	if(calledNext)
    		return null;
    	
    	int insertNum = 0;    	
    	while(dbIt.hasNext()){
    		try{
    			Tuple tempt = dbIt.next();
    			Database.getBufferPool().insertTuple(tid, tableId, tempt);
    			insertNum ++;
        	}catch (IOException e){
				e.printStackTrace();
        	}
    	}
    	Tuple t = new Tuple(getTupleDesc());
    	t.setField(0, new IntField(insertNum));
    	calledNext = true;
        return t;
    }
}
