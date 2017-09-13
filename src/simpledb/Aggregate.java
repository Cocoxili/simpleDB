package simpledb;

import java.util.*;

/**
 * The Aggregator operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends AbstractDbIterator {
	
	private DbIterator i = null;
	private DbIterator childDbIt;
	private int afield;
	private int gfield;
	private Type gfieldtype;
	private Aggregator.Op aop;
	private TupleDesc td;
	private int aggType = 0; //IntAggregate: 0; StringAggregate:1.

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
    	this.childDbIt = child;
    	this.afield = afield;
    	this.gfield = gfield;
    	this.aop = aop;
    	//no grounping
    	if(gfield == -1){
    		Type [] typeAr = new Type[1];
        	typeAr[0] = child.getTupleDesc().getType(afield);
        	this.td = new TupleDesc(typeAr);
        	
    	}else{
    		Type [] typeAr = new Type[2];
    		typeAr[0] = child.getTupleDesc().getType(gfield);
    		typeAr[1] = child.getTupleDesc().getType(afield);
    		this.td = new TupleDesc(typeAr);
        	this.gfieldtype = td.fieldType[gfield];
    	}
    	if(this.td.fieldType[1] == Type.INT_TYPE)
    		aggType = 0;
    	else if(this.td.fieldType[1] == Type.STRING_TYPE)
    		aggType = 1;
    	else
    		System.out.println("UnSupported operation: aop");
    }

    public static String aggName(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        // some code goes here
    	childDbIt.open();
    	Aggregator agt;

    	//IntAggregator
    	if(aggType == 0){
    		agt = new IntAggregator(gfield, gfieldtype, afield, aop);
    		while(childDbIt.hasNext())
    			agt.merge(childDbIt.next());
    		i = agt.iterator();
    	}
    	//StringAggregator
    	else if(aggType == 1)
    	{
    		agt = new StringAggregator(gfield, gfieldtype, afield, aop);
    		while(childDbIt.hasNext())
    			agt.merge(childDbIt.next());
    		i = agt.iterator();
    	}else
    		System.out.println("UnSupported operation.");
    	i.open();
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	if(i.hasNext())
    		return i.next();
    	else
    		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	close();
    	open();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void close() {
        // some code goes here
    	childDbIt.close();
    	i = null;
    }
}
