package simpledb;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {
	
	private int gbfield;
	private Type gbfieldtype;
	private int afield;
	private Op what;
	private DbIterator dbIt;
	private List<Tuple> tpList = new ArrayList<Tuple>();
	private List<Tuple> aggList = new ArrayList<Tuple>();
	private int tpNum;
	private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    	this.tpNum = 0;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        // some code goes here
    	//System.out.println("Before merge:");
    	//print_tpList();
    	/**no-grouping*/
    	if(gbfield == -1){
    		Type [] typeAr = new Type[1];
	    	typeAr[0] = tup.getTupleDesc().getType(afield);
	    	this.td = new TupleDesc(typeAr);
	    	tpList.add(tup);
	    	tpNum ++;
	    	return;
    	}
    	/** not no-grouping*/
    	int i = 0;
    	//找到同tup中gbfield相同的第一个元素组,并把tup插入到该组的第一个位置
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
    		System.out.printf(t.getField(0).hashCode()+"\t");
    		System.out.printf(t.getField(1).hashCode()+"\n");
    	}
    }
    
    public void intAggSum(){
    	aggList = new ArrayList<Tuple>();
    	int aggregateVal;
    	Tuple aggTp = new Tuple(td);
    	
    	int groupVal;
    	groupVal = ((IntField)(tpList.get(0).getField(gbfield))).getValue();   	   	   	   
		aggregateVal = ((IntField)(tpList.get(0).getField(afield))).getValue();
		//aggList.add(baseTp);
		int i = 1;
		for(; i < tpNum; i++){
			int gbvalue = ((IntField)(tpList.get(i).getField(gbfield))).getValue();
			int avalue = ((IntField)(tpList.get(i).getField(afield))).getValue();
			if(groupVal != gbvalue){
				aggTp.setField(gbfield, new IntField(groupVal));
	    		aggTp.setField(afield, new IntField(aggregateVal));
	    		aggList.add(aggTp);
	    		aggTp = new Tuple(td);
				groupVal = gbvalue;
				aggregateVal = avalue; 				
			}else{
				aggregateVal += avalue;
			}
		}
		aggTp.setField(gbfield, new IntField(groupVal));
		aggTp.setField(afield, new IntField(aggregateVal));
		aggList.add(aggTp);
    }
    
    public void stringAggSum(){
    	aggList = new ArrayList<Tuple>();
    	int aggregateVal;
    	Tuple aggTp = new Tuple(td);
    	
    	String groupVal;
    	groupVal = ((StringField)(tpList.get(0).getField(gbfield))).getValue();   	   	   	   
		aggregateVal = ((IntField)(tpList.get(0).getField(afield))).getValue();
		//aggList.add(baseTp);
		int i = 1;
		for(; i < tpNum; i++){
			String gbvalue = ((StringField)(tpList.get(i).getField(gbfield))).getValue();
			int avalue = ((IntField)(tpList.get(i).getField(afield))).getValue();
			if(groupVal != gbvalue){
				aggTp.setField(gbfield, new StringField(groupVal, 1024));
	    		aggTp.setField(afield, new IntField(aggregateVal));
	    		aggList.add(aggTp);
	    		aggTp = new Tuple(td);
				groupVal = gbvalue;
				aggregateVal = avalue; 				
			}else{
				aggregateVal += avalue;
			}
		}
		aggTp.setField(gbfield, new StringField(groupVal, 1024));
		aggTp.setField(afield, new IntField(aggregateVal));
		aggList.add(aggTp);
    }
    
    public void aggSum(){
    	if(gbfieldtype == Type.INT_TYPE)
    		intAggSum();
    	else if(gbfieldtype == Type.STRING_TYPE)
    		stringAggSum();
    }
    public void aggMin(){
    	aggList = new ArrayList<Tuple>();
    	int groupVal;
    	int aggregateVal;
    	   	
    	Tuple aggTp = new Tuple(td);
		groupVal = ((IntField)(tpList.get(0).getField(gbfield))).getValue();
		aggregateVal = ((IntField)(tpList.get(0).getField(afield))).getValue();

		int i = 1;
		for(; i < tpNum; i++){
			int gbvalue = ((IntField)(tpList.get(i).getField(gbfield))).getValue();
			int avalue = ((IntField)(tpList.get(i).getField(afield))).getValue();
			if(groupVal != gbvalue){
				aggTp.setField(gbfield, new IntField(groupVal));
	    		aggTp.setField(afield, new IntField(aggregateVal));
	    		aggList.add(aggTp);
	    		aggTp = new Tuple(td);
				groupVal = gbvalue;
				aggregateVal = avalue; 				
			}else{
				if(avalue < aggregateVal)
					aggregateVal = avalue;
			}
		}
		aggTp.setField(gbfield, new IntField(groupVal));
		aggTp.setField(afield, new IntField(aggregateVal));
		aggList.add(aggTp);
    }
    
    public void aggMax(){
    	aggList = new ArrayList<Tuple>();
    	int groupVal;
    	int aggregateVal;
    	   	
    	Tuple aggTp = new Tuple(td);
		groupVal = ((IntField)(tpList.get(0).getField(gbfield))).getValue();
		aggregateVal = ((IntField)(tpList.get(0).getField(afield))).getValue();

		int i = 1;
		for(; i < tpNum; i++){
			int gbvalue = ((IntField)(tpList.get(i).getField(gbfield))).getValue();
			int avalue = ((IntField)(tpList.get(i).getField(afield))).getValue();
			if(groupVal != gbvalue){
				aggTp.setField(gbfield, new IntField(groupVal));
	    		aggTp.setField(afield, new IntField(aggregateVal));
	    		aggList.add(aggTp);
	    		aggTp = new Tuple(td);
				groupVal = gbvalue;
				aggregateVal = avalue; 				
			}else{
				if(avalue > aggregateVal)
					aggregateVal = avalue;
			}
		}
		aggTp.setField(gbfield, new IntField(groupVal));
		aggTp.setField(afield, new IntField(aggregateVal));
		aggList.add(aggTp);
    }
    
    public void aggAvgNoGrouping(){
    	aggList = new ArrayList<Tuple>();
    	Tuple aggTp = new Tuple(td);
    	int sum = 0;
    	for(int j = 0; j < tpNum; j++)
    		sum += ((IntField)tpList.get(j).getField(afield)).getValue();
    	aggTp.setField(0, new IntField(sum/tpNum));
    	aggList.add(aggTp);
    	   	
    }
    public void aggAvg(){
    	aggList = new ArrayList<Tuple>();
    	int groupVal;
    	int aggregateVal;
    	int aggNum = 0;
    	Tuple aggTp = new Tuple(td);
    	
		groupVal = ((IntField)(tpList.get(0).getField(gbfield))).getValue();
		aggregateVal = ((IntField)(tpList.get(0).getField(afield))).getValue();
		aggNum ++;
		
		int i = 1;
		for(; i < tpNum; i++){
			int gbvalue = ((IntField)(tpList.get(i).getField(gbfield))).getValue();
			int avalue = ((IntField)(tpList.get(i).getField(afield))).getValue();
			if(groupVal != gbvalue){
				aggTp.setField(gbfield, new IntField(groupVal));
	    		aggTp.setField(afield, new IntField(aggregateVal/aggNum));
	    		aggList.add(aggTp);
	    		aggTp = new Tuple(td);
				groupVal = gbvalue;
				aggregateVal = avalue; 		
				aggNum = 1;
			}else{
				aggregateVal += avalue;
				aggNum ++;
			}
		}
		aggTp.setField(gbfield, new IntField(groupVal));
		aggTp.setField(afield, new IntField(aggregateVal/aggNum));
		aggList.add(aggTp);
    }
    
    public void aggCount(){
    	aggList = new ArrayList<Tuple>();
    	int groupVal;
    	int aggNum = 0;
    	
    	Tuple aggTp = new Tuple(td);
		groupVal = ((IntField)(tpList.get(0).getField(gbfield))).getValue();
		aggNum ++;
		
		int i = 1;
		for(; i < tpNum; i++){
			int gbvalue = ((IntField)(tpList.get(i).getField(gbfield))).getValue();
			if(groupVal != gbvalue){
				aggTp.setField(gbfield, new IntField(groupVal));
	    		aggTp.setField(afield, new IntField(aggNum));
	    		aggList.add(aggTp);
	    		aggTp = new Tuple(td);
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
    	switch(what){
    	case SUM: aggSum();
    		break;
    	case MIN: aggMin();
    		break;
    	case MAX: aggMax();
    		break;
    	case AVG: 
    		if(gbfield == -1)
    			aggAvgNoGrouping();
    		else
    			aggAvg();
			break;
    	case COUNT: aggCount();
    		break;
    	default:
    		System.out.println("Unsupported operation\n");
    	}
    	dbIt = new TupleIterator(td, aggList);
    	return dbIt;
        //throw new UnsupportedOperationException("implement me");
    }

}
