package simpledb.systemtest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;

import simpledb.*;

public class SystemTestUtil {
    public static final TupleDesc SINGLE_INT_DESCRIPTOR =
            new TupleDesc(new Type[]{Type.INT_TYPE});

    private static final int MAX_RAND_VALUE = 1 << 16;

    /** @param columnSpecification Mapping between column index and value. */
    public static HeapFile createRandomHeapFile(
            int columns, int rows, Map<Integer, Integer> columnSpecification,
            ArrayList<ArrayList<Integer>> tuples)
            throws IOException, DbException, TransactionAbortedException {
        return createRandomHeapFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples);
    }

    /** @param columnSpecification Mapping between column index and value. */
    public static HeapFile createRandomHeapFile(
            int columns, int rows, int maxValue, Map<Integer, Integer> columnSpecification,
            ArrayList<ArrayList<Integer>> tuples)
            throws IOException, DbException, TransactionAbortedException {
        File temp = createRandomHeapFileUnopened(columns, rows, maxValue,
                columnSpecification, tuples);
        return Utility.openHeapFile(columns, temp);
    }

    public static File createRandomHeapFileUnopened(int columns, int rows,
            int maxValue, Map<Integer, Integer> columnSpecification,
            ArrayList<ArrayList<Integer>> tuples) throws IOException {
        if (tuples != null) {
            tuples.clear();
        } else {
            tuples = new ArrayList<ArrayList<Integer>>(rows);
        }

        Random r = new Random();

        // Fill the tuples list with generated values
        for (int i = 0; i < rows; ++i) {
            ArrayList<Integer> tuple = new ArrayList<Integer>(columns);
            for (int j = 0; j < columns; ++j) {
                // Generate random values, or use the column specification
                Integer columnValue = null;
                if (columnSpecification != null) columnValue = columnSpecification.get(j);
                if (columnValue == null) {
                    columnValue = r.nextInt(maxValue);
                }
                tuple.add(columnValue);
            }
            tuples.add(tuple);
        }

        // Convert the tuples list to a heap file and open it
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.PAGE_SIZE, columns);
        return temp;
    }

    public static ArrayList<Integer> tupleToList(Tuple tuple) {
        ArrayList<Integer> list = new ArrayList<Integer>();
        for (int i = 0; i < tuple.getTupleDesc().numFields(); ++i) {
            int value = ((IntField)tuple.getField(i)).getValue();
            list.add(value);
        }
        return list;
    }

    public static void matchTuples(DbFile f, List<ArrayList<Integer>> tuples)
            throws DbException, TransactionAbortedException, IOException {
        TransactionId tid = new TransactionId();
        matchTuples(f, tid, tuples);
        Database.getBufferPool().transactionComplete(tid);
    }

    public static void matchTuples(DbFile f, TransactionId tid, List<ArrayList<Integer>> tuples)
            throws DbException, TransactionAbortedException, IOException {
        SeqScan scan = new SeqScan(tid, f.getId(), "");
        matchTuples(scan, tuples);
    }

    public static void matchTuples(DbIterator iterator, List<ArrayList<Integer>> tuples)
            throws DbException, TransactionAbortedException, IOException {
        ArrayList<ArrayList<Integer>> copy = new ArrayList<ArrayList<Integer>>(tuples);

        if (Debug.isEnabled()) {
            Debug.log("Expected tuples:");
            for (ArrayList<Integer> t : copy) {
                Debug.log("\t" + Utility.listToString(t));
            }
        }

        iterator.open();
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            ArrayList<Integer> list = tupleToList(t);
            boolean isExpected = copy.remove(list);
            Debug.log("scanned tuple: %s (%s)", t, isExpected ? "expected" : "not expected");
            if (!isExpected) {
                Assert.fail("expected tuples does not contain: " + t);
            }
        }
        iterator.close();

        if (!copy.isEmpty()) {
            String msg = "expected to find the following tuples:\n";
            final int MAX_TUPLES_OUTPUT = 10;
            int count = 0;
            for (ArrayList<Integer> t : copy) {
                if (count == MAX_TUPLES_OUTPUT) {
                    msg += "[" + (copy.size() - MAX_TUPLES_OUTPUT) + " more tuples]";
                    break;
                }
                msg += "\t" + Utility.listToString(t) + "\n";
                count += 1;
            }
            Assert.fail(msg);
        }
    }

    /**
     * Returns number KB of RAM used by JVM after calling System.gc many times.
     * @return KB of RAM used by JVM
     */
    public static long getMemoryFootprint() {
        // Call System.gc in a loop until it stops freeing memory. This is
        // still no guarantee that all the memory is freed, since System.gc is
        // just a "hint".
        Runtime runtime = Runtime.getRuntime();
        long memAfter = runtime.totalMemory() - runtime.freeMemory();
        long memBefore = memAfter + 1;
        while (memBefore != memAfter) {
            memBefore = memAfter;
            System.gc();
            memAfter = runtime.totalMemory() - runtime.freeMemory();
        }

        return memAfter;
    }
}
