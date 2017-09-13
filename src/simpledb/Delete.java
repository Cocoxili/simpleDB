package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {
	
	private TransactionId tid;
	private DbIterator child;
	private boolean calledNext = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
    	Type [] ty = new Type[1];
    	ty[0] = Type.INT_TYPE;
        return new TupleDesc(ty);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.open();
    }

    public void close() {
        // some code goes here
    	child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be access via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	
    	//return null if called this method more than once
    	if(calledNext)
    		return null;
    	
    	int deleteNum = 0;    	
    	while(child.hasNext()){
    		Tuple tempt = child.next();
    		Database.getBufferPool().deleteTuple(tid, tempt);
    		deleteNum ++;
    	}
    	Tuple t = new Tuple(getTupleDesc());
    	t.setField(0, new IntField(deleteNum));
    	calledNext = true;
        return t;
    }
}
