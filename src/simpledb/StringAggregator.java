package simpledb;

import java.util.ArrayList;
import java.util.List;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private DbIterator dbIt;
	private List<Tuple> tpList = new ArrayList<Tuple>();
	private List<Tuple> aggList = new ArrayList<Tuple>();
	private int tpNum;
	private TupleDesc td;
	private TupleDesc tdInt;
	
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	this.tpNum = 0;
    	//count返回值的tuple两列都是Int值
    	Type [] typeAr = new Type[2];
    	typeAr[0] = Type.INT_TYPE;
    	typeAr[1] = Type.INT_TYPE;
    	this.tdInt = new TupleDesc(typeAr);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	//System.out.println("Before merge:");
    	//print_tpList();
    	
    	int i = 0;
    	/**找到同tup中gbfield相同的第一个元素组,并把tup插入到该组的第一个位置*/
    	for(i = 0; i < tpNum; i++){
    		Tuple tempt = tpList.get(i);
    		if(tempt.getField(gbfield).equals(tup.getField(gbfield))){
    			tpList.add(i, tup);
    			tpNum ++;
    			break;
    		}
    	}
    	if(i == tpNum){
    		tpList.add(i, tup);
			tpNum ++;
			//this.td = tup.getTupleDesc();
	    	Type [] typeAr = new Type[2];
	    	typeAr[0] = gbfieldtype;
	    	typeAr[1] = tup.getTupleDesc().getType(afield);
	    	this.td = new TupleDesc(typeAr);
    	}
    	
    	
    	//System.out.println("After merge:");
    	//print_tpList();
    }

    // method for debug
    public void print_tpList() {
    	for(int i = 0; i < tpNum; i++){
    		Tuple t = tpList.get(i);
    		System.out.printf(((IntField)t.getField(0)).getValue()+"\t");
    		System.out.printf(((StringField)t.getField(1)).getValue()+"\n");
    	}
    }
    
    public void aggCount(){
    	aggList = new ArrayList<Tuple>();
    	int groupVal;
    	int aggNum = 0;
    	Tuple aggTp = new Tuple(tdInt);
		groupVal = ((IntField)(tpList.get(0).getField(gbfield))).getValue();
		aggNum ++;
		
		int i = 1;
		for(; i < tpNum; i++){
			int gbvalue = ((IntField)(tpList.get(i).getField(gbfield))).getValue();
			if(groupVal != gbvalue){
				aggTp.setField(gbfield, new IntField(groupVal));
	    		aggTp.setField(afield, new IntField(aggNum));
	    		aggList.add(aggTp);
	    		aggTp = new Tuple(tdInt);
				groupVal = gbvalue;
				aggNum = 1;
			}else{
				aggNum ++;
			}
		}
		aggTp.setField(gbfield, new IntField(groupVal));
		aggTp.setField(afield, new IntField(aggNum));
		aggList.add(aggTp);
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	aggList = new ArrayList<Tuple>();
    	//int groupVal;
    	//int aggregateVal;
    	//Tuple aggTp = new Tuple(td);
    	switch(what){
    	case COUNT: aggCount();
    		break;
    	default:
    		System.out.println("Unsupported operation\n");
    	}
    	dbIt = new TupleIterator(tdInt, aggList);
    	return dbIt;
        //throw new UnsupportedOperationException("implement me");
    }

}
