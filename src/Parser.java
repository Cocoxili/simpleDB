
import Zql.*;
import simpledb.*;
import java.io.*;
import java.util.*;

import jline.ArgumentCompletor;
import jline.ConsoleReader;
import jline.SimpleCompletor;

public class Parser {

    static Predicate.Op getOp(String s) throws ParsingException {
        if (s.equals("=")) return Predicate.Op.EQUALS;
        if (s.equals(">")) return Predicate.Op.GREATER_THAN;
        if (s.equals(">=")) return Predicate.Op.GREATER_THAN_OR_EQ;
        if (s.equals("<")) return Predicate.Op.LESS_THAN;
        if (s.equals("<=")) return Predicate.Op.LESS_THAN_OR_EQ;
        if (s.equals("LIKE")) return Predicate.Op.LIKE;
        throw new ParsingException("Unknown predicate " + s);
    }

    static Aggregator.Op getAggOp(String s) throws ParsingException {
        s = s.toUpperCase();
        if (s.equals("AVG")) return Aggregator.Op.AVG;
        if (s.equals("SUM")) return Aggregator.Op.SUM;
        if (s.equals("COUNT")) return Aggregator.Op.COUNT;
        if (s.equals("MIN")) return Aggregator.Op.MIN;
        if (s.equals("MAX")) return Aggregator.Op.MAX;
        throw new ParsingException("Unknown predicate " + s);
    }

    static void processExpression(TransactionId tid, ZExpression wx, HashMap<String,DbIterator> tableMap, HashMap<String,String> equivMap) throws ParsingException {
        if (wx.getOperator().equals("AND")) {
            for (int i = 0; i < wx.nbOperands(); i++) {
                if (!(wx.getOperand(i) instanceof ZExpression)) {
                    throw new ParsingException("Nested queries are currently unsupported.");
                }
                ZExpression newWx = (ZExpression)wx.getOperand(i);
                processExpression(tid, newWx, tableMap, equivMap);

            }
        } else if (wx.getOperator().equals("OR")) {
            throw new ParsingException("OR expressions currently unsupported.");
        } else {
            // this is a binary expression comparing two constants
            @SuppressWarnings("unchecked")
            Vector<ZExp> ops = wx.getOperands();
            if (ops.size() != 2) {
                throw new ParsingException("Only simple binary expresssions of the form A op B are currently supported.");
            }

            boolean isJoin = false;
            Predicate.Op op = getOp(wx.getOperator());

            boolean op1const = ops.elementAt(0) instanceof ZConstant; //otherwise is a Query
            boolean op2const = ops.elementAt(1) instanceof ZConstant; //otherwise is a Query
            if (op1const && op2const) {
                isJoin  = ((ZConstant)ops.elementAt(0)).getType() == ZConstant.COLUMNNAME &&  ((ZConstant)ops.elementAt(1)).getType() == ZConstant.COLUMNNAME;
            } else if (ops.elementAt(0) instanceof ZQuery || ops.elementAt(1) instanceof ZQuery) {
                isJoin = true;
            } else if (ops.elementAt(0) instanceof ZExpression || ops.elementAt(1) instanceof ZExpression) {
                throw new ParsingException("Only simple binary expresssions of the form A op B are currently supported, where A or B are fields, constants, or subqueries.");
            } else isJoin = false;

            if (isJoin) {       //join node
                PredicateInfo p1info = new PredicateInfo();
                p1info.checkJoinPredicate(tid, op1const, ops.elementAt(0), tableMap, equivMap);

                PredicateInfo p2info = new PredicateInfo();
                p2info.checkJoinPredicate(tid, op2const, ops.elementAt(1), tableMap, equivMap);

                if (!op2const) {  //swap operators so that nested query is not the inner (that's bad)
                    DbIterator tmp =p1info.oldNode;
                    p1info.oldNode = p2info.oldNode;
                    p2info.oldNode = tmp;
                }

                TupleDesc td1 = p1info.oldNode.getTupleDesc();
                TupleDesc td2 = p2info.oldNode.getTupleDesc();

                JoinPredicate jp;
                try {
                    jp = new JoinPredicate(p1info.fieldId, op, p2info.fieldId);
                } catch (NoSuchElementException e) {
                    throw new ParsingException(e);
                }

                if (p1info.oldNode == null) {
                    throw new ParsingException(p1info.tabfieldar[0] + " does not appear in FROM list");
                }
                if (p2info.oldNode == null) {
                    throw new ParsingException(p2info.tabfieldar[0] + " does not appear in FROM list");
                }

                DbIterator join;
                if (op == Predicate.Op.EQUALS) {
                    try {
                        //dynamically load HashEquiJoin -- if it doesn't exist, just fall back on regular join
                        Class<?> c = Class.forName("simpledb.HashEquiJoin");
                        java.lang.reflect.Constructor<?> ct = c.getConstructors()[0];
                        join = (DbIterator)ct.newInstance(new Object[]{jp,p1info.oldNode,p2info.oldNode});
                    } catch (Exception e) {
                        join = new Join(jp, p1info.oldNode, p2info.oldNode);
                    }
                } else {
                    join = new Join(jp, p1info.oldNode, p2info.oldNode);
                }

                if (op1const) {
                    tableMap.put(p1info.tabfieldar[0], join);
                } else
                    tableMap.put(p2info.tabfieldar[0], join);

                System.out.println("ADDED JOIN NODE OVER t1 = " + td1.getFieldName(p1info.fieldId) + ", t2 = " + td2.getFieldName(p2info.fieldId));

                if (op2const && op1const) { //nested queries don't have a table in tableMap so don't have to remove anything
                    tableMap.remove(p2info.tabfieldar[0]);

                    equivMap.put(p2info.tabfieldar[0], p1info.tabfieldar[0]);  //keep track of the fact that this new node contains both tables
                    //make sure anything that was equiv to p2info.tabfieldar (which we are just removed) is
                    // marked as equiv to p1info.tabfieldar (which we are replacing p2info.tabfieldar with.)
                    for (Map.Entry<String, String> s: equivMap.entrySet()) {
                        String val = s.getValue();
                        if (val.equals(p2info.tabfieldar[0])) {
                            s.setValue(p1info.tabfieldar[0]);
                        }
                    }
                }

            } else { //select node
                String column;
                String compValue;
                ZConstant op1 = (ZConstant)ops.elementAt(0);
                ZConstant op2 = (ZConstant)ops.elementAt(1);
                if (op1.getType() == ZConstant.COLUMNNAME) {
                    column = op1.getValue();
                    compValue = new String(op2.getValue());
                } else {
                    column = op2.getValue();
                    compValue = new String(op1.getValue());
                }
                String[] split = column.split("[.]");
                if (split.length != 2) {
                    throw new ParsingException("Expression " + column + " does not conform to TABLENAME.FIELDNAME syntax.");
                }
                String name = split[0];
                if (equivMap.get(name) != null)
                    name = equivMap.get(name);
                DbIterator oldNode = tableMap.get(name);
                if (oldNode == null) {
                    throw new ParsingException("Unknown table " + name);
                }
                TupleDesc td = oldNode.getTupleDesc();

                Field f;
                Type ftyp;
                try {
                    ftyp = td.getType(td.nameToId(column));
                } catch (NoSuchElementException e) {
                    throw new ParsingException(e);
                }
                if (ftyp == Type.INT_TYPE)
                     f = new IntField(new Integer(compValue).intValue());
                else
                    f = new StringField(compValue, Type.STRING_LEN);

                Predicate pred;
                try {
                    pred = new Predicate(td.nameToId(column), op, f);
                } catch (NoSuchElementException e) {
                    throw new ParsingException(e);
                }

                System.out.println("ADDED SELECT NODE OVER " + column + "(" + pred + ")");

                Filter filt = new Filter(pred, oldNode);
                tableMap.put(name, filt);
            }
        }

    }

    public static DbIterator parseQuery(TransactionId tid, ZQuery q) throws IOException, Zql.ParseException, ParsingException {
        @SuppressWarnings("unchecked")
        Vector<ZFromItem> from = q.getFrom();
        HashMap<String,DbIterator> tableMap = new HashMap<String,DbIterator>();
        HashMap<String,String> equivMap = new HashMap<String,String>();

        //walk through tables in the FROM clause
        for (int i = 0; i < from.size(); i++) {
            ZFromItem fromIt = from.elementAt(i);
            try {
                int id = Database.getCatalog().getTableId(fromIt.getTable()); //will fall through if table doesn't exist
                String name;

                if (fromIt.getAlias() != null)
                    name = fromIt.getAlias();
                else
                    name = fromIt.getTable();
                System.out.println(" ADDING TABLE " + name + "(" + fromIt.getTable() + ") TO tableMap");
                //replace the TupleDesc in it with the same tuple desc where every field name is prefaced by the table alias
                DbIterator it = new SeqScan(tid,id, name);
                tableMap.put(name, it);
                System.out.println("     TABLE HAS  tupleDesc " + it.getTupleDesc());
            } catch (NoSuchElementException e) {
                throw new ParsingException("Table " + fromIt.getTable() + " is not in catalog");
            }
        }

        // now parse the where clause, creating Filter and Join nodes as needed
        ZExp w = q.getWhere();
        if (w != null) {
            System.out.println("WHERE = " + q.getWhere());

            if (!(w instanceof ZExpression)) {
                throw new ParsingException("Nested queries are currently unsupported.");
            }
            ZExpression wx = (ZExpression)w;
            processExpression(tid, wx, tableMap, equivMap);

            if (tableMap.size() > 1) {
                throw new ParsingException("Query does not include join expressions joining all nodes!");
            }
        }

        DbIterator node = (DbIterator)(tableMap.entrySet().iterator().next().getValue());

        // now look for group by fields
        ZGroupBy gby = q.getGroupBy();
        String groupByField = null;
        if (gby != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> gbs = gby.getGroupBy();
            if (gbs.size() > 1) {
                throw new ParsingException("At most one grouping field expression supported.");
            }
            if (gbs.size() == 1) {
                ZExp gbe = gbs.elementAt(0);
                if (! (gbe instanceof ZConstant)) {
                    throw new ParsingException("Complex grouping expressions (" + gbe + ") not supported.");
                }
                groupByField = ((ZConstant)gbe).getValue();
                System.out.println ("GROUP BY FIELD : " + groupByField);
            }

        }

        // walk the select list, pick out aggregates, and check for query validity
        @SuppressWarnings("unchecked")
        Vector<ZSelectItem> selectList = q.getSelect();
        String aggField = null;
        String aggFun = null;

        for (int i = 0; i < selectList.size(); i++) {
            ZSelectItem si = selectList.elementAt(i);
            if (si.getAggregate() == null && (si.isExpression() && !(si.getExpression() instanceof ZConstant))) {
                throw new ParsingException("Expressions in SELECT list are not supported.");
            }
            if (si.getAggregate() != null) {
                if (aggField != null) {
                    throw new ParsingException("Aggregates over multiple fields not supported.");
                }
                aggField = ((ZConstant)((ZExpression)si.getExpression()).getOperand(0)).getValue();
                aggFun = si.getAggregate();
                System.out.println ("Aggregate field is " + aggField + ", agg fun is : " + aggFun);
            } else {
                if (groupByField != null && !groupByField.equals(si.getTable() + "." + si.getColumn())) {
                    throw new ParsingException("Non-aggregate field " + si.getColumn() + " does not appear in GROUP BY list.");
                }
            }
        }

        if (groupByField != null && aggFun == null) {
            throw new ParsingException("GROUP BY without aggregation.");
        }

        //walk the select list again, to determine order in which to project output fields
        ArrayList<Integer> outFields = new ArrayList<Integer>();
        ArrayList<Type> outTypes = new ArrayList<Type>();
        for (int i = 0; i < selectList.size(); i++) {
            ZSelectItem si = selectList.elementAt(i);
            if (si.getAggregate() != null) {
                outFields.add(groupByField!=null?1:0);
                TupleDesc td = node.getTupleDesc();
                int  id;
                try {
                    id = td.nameToId(aggField);
                } catch (NoSuchElementException e) {
                    throw new ParsingException(e);
                }
                outTypes.add(Type.INT_TYPE);  //the type of all aggregate functions is INT

            } else {
                if (aggField != null) {
                    if (groupByField == null) {
                        throw new ParsingException("Field " + si + " does not appear in GROUP BY list");
                    }
                    outFields.add(0);
                    TupleDesc td = node.getTupleDesc();
                    int  id;
                    try {
                        id = td.nameToId(groupByField);
                    } catch (NoSuchElementException e) {
                        throw new ParsingException(e);
                    }
                    outTypes.add(td.getType(id));
                } else {
                    TupleDesc td = node.getTupleDesc();
                    int id;
                    try {
                        id = td.nameToId(si.getTable() + "." + si.getColumn());
                    } catch (NoSuchElementException e) {
                        throw new ParsingException(e);
                    }
                    outFields.add(id);
                    outTypes.add(td.getType(id));

                }
            }
        }

        //construct aggregate node
        if (aggFun != null) {
            TupleDesc td = node.getTupleDesc();
            Aggregate aggNode;
            try {
                aggNode = new Aggregate(node,
                        td.nameToId(aggField),
                        groupByField == null?Aggregator.NO_GROUPING:td.nameToId(groupByField),
                                getAggOp(aggFun));
            } catch (NoSuchElementException e) {
                throw new ParsingException(e);
            }
            node = aggNode;
        }

        // sort the data

        if (q.getOrderBy() != null) {
            @SuppressWarnings("unchecked")
                Vector<ZOrderBy> obys = q.getOrderBy();
            if (obys.size() > 1)  {
                throw new ParsingException("Multi-attribute ORDER BY is not supported.");
            }
            ZOrderBy oby = obys.elementAt(0);
            if (!(oby.getExpression() instanceof ZConstant)) {
                throw new ParsingException("Complex ORDER BY's are not supported");
            }
            ZConstant f = (ZConstant)oby.getExpression();
            node = new OrderBy(node.getTupleDesc().nameToId(f.getValue()), oby.getAscOrder(), node);

        }
        return new Project(outFields,outTypes, node);

    }

    static Transaction curtrans = null;

    public static void handleQueryStatement(ZQuery s) throws TransactionAbortedException, DbException, IOException, ParsingException, Zql.ParseException  {
        // and run it
        DbIterator node;
        node = parseQuery(curtrans.getId(), s);

        Query sdbq = new Query(node, curtrans.getId());
        // XXX print field names
        sdbq.start();
        int cnt = 0;
        while (sdbq.hasNext()) {
            Tuple tup = sdbq.next();
            System.out.println(tup);
            cnt++;
        }
        System.out.println("\n " + cnt + " rows.");
        sdbq.close();
    }

    public static void handleInsertStatement(ZInsert s) throws TransactionAbortedException, DbException, IOException, ParsingException, Zql.ParseException  {
        int id;
        try {
            id = Database.getCatalog().getTableId(s.getTable()); //will fall through if table doesn't exist
        } catch (NoSuchElementException e) {
            throw new ParsingException ("Unknown table : " + s.getTable());
        }

        TupleDesc td = Database.getCatalog().getTupleDesc(id);

        Tuple t = new Tuple(td);
        int i = 0;
        DbIterator newTups;

        if (s.getValues() != null) {
            @SuppressWarnings("unchecked")
            Vector<ZExp> values = (Vector<ZExp>)s.getValues();
            if (td.numFields() != values.size()) {
                throw new ParsingException("INSERT statement does not contain same number of fields as table " + s.getTable());
            }
            for (ZExp e : values) {

                if (!(e instanceof ZConstant))
                    throw new ParsingException("Complex expressions not allowed in INSERT statements.");
                ZConstant zc = (ZConstant)e;
                if (zc.getType() == ZConstant.NUMBER) {
                    if (td.getType(i) != Type.INT_TYPE) {
                        throw new ParsingException("Value " + zc.getValue() + " is not an integer.");
                    }
                    IntField f= new IntField(new Integer(zc.getValue()));
                    t.setField(i,f);
                } else if(zc.getType() == ZConstant.STRING) {
                    if (td.getType(i) != Type.STRING_TYPE) {
                        throw new ParsingException("Value " + zc.getValue() + " is not a string.");
                    }
                    StringField f= new StringField(zc.getValue(), Type.STRING_LEN);
                    t.setField(i,f);
                } else {
                    throw new ParsingException("Only string or int fields are supported.");
                }

                i ++;
            }
            ArrayList<Tuple> tups = new ArrayList<Tuple>();
            tups.add(t);
            newTups = new TupleArrayIterator(tups);

        } else {
            ZQuery query = (ZQuery)s.getQuery();
            newTups = parseQuery(curtrans.getId(),query);
        }

        Query sdbq = new Query(new Insert(curtrans.getId(), newTups, id), curtrans.getId());
        // XXX print field names
        sdbq.start();
        System.out.print("Inserted ");
        while (sdbq.hasNext()) {
            Tuple tup = sdbq.next();
            System.out.println(tup);
        }
        sdbq.close();

    }

    public static void handleDeleteStatement(ZDelete s) throws TransactionAbortedException, DbException, IOException, ParsingException, Zql.ParseException  {
        HashMap<String, DbIterator> tableMap = new HashMap<String, DbIterator>();

        int id;
        try {
            id = Database.getCatalog().getTableId(s.getTable()); //will fall through if table doesn't exist
        } catch (NoSuchElementException e) {
            throw new ParsingException ("Unknown table : " + s.getTable());
        }
        String name = s.getTable();;

        System.out.println(" ADDING TABLE " + name + " TO tableMap");
        //replace the TupleDesc in it with the same tuple desc where every field name is prefaced by the table alias
        DbIterator it = new SeqScan(curtrans.getId(),id,name);
        tableMap.put(name, it);
        System.out.println("     TABLE HAS  tupleDesc " + it.getTupleDesc());

        if (s.getWhere() != null)
            processExpression( curtrans.getId(), (ZExpression)s.getWhere(), tableMap, new HashMap<String,String>());

        Query sdbq = new Query(new Delete(curtrans.getId(), tableMap.get(name)), curtrans.getId());
        // XXX print field names
        sdbq.start();
        System.out.print("Deleted ");
        while (sdbq.hasNext()) {
            Tuple tup = sdbq.next();
            System.out.println(tup);
        }
        sdbq.close();

    }

    public static void handleTransactStatement(ZTransactStmt s) throws TransactionAbortedException, DbException, IOException, ParsingException, Zql.ParseException {
        if (s.getStmtType().equals("COMMIT")) {
            curtrans.transactionComplete(false);
            curtrans = null;
            System.out.println("Transaction committed.");
        } else if (s.getStmtType().equals("ROLLBACK")) {
            curtrans.transactionComplete(true);
            curtrans = null;
            System.out.println("Transaction aborted.");

        } else {
            throw new ParsingException("Can't start new transactions until current transaction has been committed or rolledback.");
        }
    }

    public static void processNextStatement(InputStream is) {
        try {
            ZqlParser p = new ZqlParser(is);
            ZStatement s = p.readStatement();

            if (s instanceof ZTransactStmt)
                handleTransactStatement((ZTransactStmt)s);
            else if (s instanceof ZInsert)
                handleInsertStatement((ZInsert)s);
            else if (s instanceof ZDelete)
                handleDeleteStatement((ZDelete)s);
            else if (s instanceof ZQuery)
                handleQueryStatement((ZQuery)s);
            else {
                System.out.println("Can't parse " + s + "\n -- parser only handles SQL transactions, insert, delete, and select statements");
            }

        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParsingException e) {
            System.out.println("Invalid SQL expression: \n \t" + e.getMessage());
        } catch (Zql.ParseException e) {
            System.out.println("Invalid SQL expression: \n \t " + e);
        }
    }

    // Basic SQL completions
    static final String[] SQL_COMMANDS = {
        "select",
        "from",
        "where",
        "group by",
        "max(",
        "min(",
        "avg(",
        "count",
        "rollback",
        "commit",
        "insert",
        "delete",
        "values",
        "into",
    };

    public static void main(String argv[]) throws IOException {
        String usage = "Usage: parser catalogFile [queryFile]";

        if (argv.length < 1 || argv.length > 2) {
            System.out.println("Invalid number of arguments.\n" + usage);
            System.exit(0);
        }

        //first add tables to database
        Database.getCatalog().loadSchema(argv[0]);

        if (argv.length == 2) {
            try {
                curtrans = new Transaction();
                curtrans.start();
                processNextStatement(new FileInputStream(new File(argv[1])));
            } catch (FileNotFoundException e) {
                System.out.println("Unable to find query file");
                e.printStackTrace();
            }
        } else { // no query file, run interactive prompt
            ConsoleReader reader = new ConsoleReader();

            // Add really stupid tab completion for simple SQL
            ArgumentCompletor completor = new ArgumentCompletor(new SimpleCompletor(SQL_COMMANDS));
            completor.setStrict(false);  // match at any position
            reader.addCompletor(completor);

            StringBuilder buffer = new StringBuilder();
            String line;
            while ((line = reader.readLine("SimpleDB> ")) != null) {
                // Split statements at ';': handles multiple statements on one line, or one
                // statement spread across many lines
                while (line.indexOf(';') >= 0) {
                    int split = line.indexOf(';');
                    buffer.append(line.substring(0, split+1));
                    byte[] statementBytes = buffer.toString().getBytes("UTF-8");

                    //create a transaction for the query
                    if (curtrans == null) {
                        curtrans = new Transaction();
                        curtrans.start();
                        System.out.println("Started a new transaction tid = " + curtrans.getId().getId());
                    }
                    long startTime = System.currentTimeMillis();
                    processNextStatement(new ByteArrayInputStream(statementBytes));
                    long time = System.currentTimeMillis() - startTime;
                    System.out.printf("----------------\n%.2f seconds\n\n", ((double)time/1000.0));
                    // Grab the remainder of the line
                    line = line.substring(split+1);
                    buffer = new StringBuilder();
                }
                if (line.length() > 0) {
                    buffer.append(line);
                    buffer.append("\n");
                }
            }
        }
    }
}

/*
   Predicate info checks a predicate to a join
   for validity and constructs a dbIterator for
   it.
*/
class PredicateInfo {
    public String tabfieldar[] = null;
    public DbIterator oldNode;
    public int fieldId;

    public PredicateInfo() {
    }

    public void checkJoinPredicate( TransactionId tid, boolean isTableField, ZExp exp,  HashMap<String,DbIterator> tableMap, HashMap<String,String> equivMap ) throws ParsingException {
        if (isTableField) { //const means this is a column from a table
            ZConstant node = (ZConstant)exp;
            String col = node.getValue();
            tabfieldar = col.split("[.]");
            if (tabfieldar.length != 2) {
                throw new ParsingException("Expression " + col + " does not conform to TABLENAME.FIELDNAME syntax.");
            }
            if (equivMap.get(tabfieldar[0]) != null) {
                System.out.println (tabfieldar[0] + " is equivalent to " + equivMap.get(tabfieldar[0]));
                tabfieldar[0] = equivMap.get(tabfieldar[0]);
            }
            oldNode = tableMap.get(tabfieldar[0]);
            if (oldNode == null) {
                throw new ParsingException("Unknown table : " + tabfieldar[0] + " in join expression.");
            }
            fieldId = oldNode.getTupleDesc().nameToId(col);
        } else {  //non const means this is a subquery
            try {
                oldNode = Parser.parseQuery(tid, (ZQuery)exp);
            } catch (IOException e) {
                throw new ParsingException(e);
            } catch (ParseException e) {
                throw new ParsingException(e);
            }
            fieldId = 0;
        }
    }
}

class ParsingException extends Exception {
    public ParsingException(String string) {
        super(string);
    }

    public ParsingException(Exception e) {
        super(e);
    }

    /**
     *
     */
    private static final long serialVersionUID = 1L;
}

class TupleArrayIterator implements DbIterator {
    ArrayList<Tuple> tups;
    Iterator<Tuple> it = null;

    public TupleArrayIterator(ArrayList<Tuple> tups) {
        this.tups = tups;
    }

    public void open()
        throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /** @return true if the iterator has more items. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator, or null if there are no more tuples.

     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        return it.next();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        it = tups.iterator();
    }

    /**
     * Returns the TupleDesc associated with this DbIterator.
     */
    public TupleDesc getTupleDesc() {
        return tups.get(0).getTupleDesc();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
    }

}
