import simpledb.*;
import java.util.*;
import java.io.*;

public class SimpleDb {
    public static void main (String args[])
            throws DbException, TransactionAbortedException, IOException {
        // convert a file
        if(args[0].equals("convert")) {
        try {
        if (args.length == 3) {
            HeapFileEncoder.convert(new File(args[1]),
                        new File(args[1].replaceAll(".txt", ".dat")),
                        BufferPool.PAGE_SIZE,
                        Integer.parseInt(args[2]));
        }
        else if (args.length == 4) {
            ArrayList<Type> ts = new ArrayList<Type>();
            String[] typeStringAr = args[3].split(",");
            for (String s: typeStringAr) {
            if (s.toLowerCase().equals("int"))
                ts.add(Type.INT_TYPE);
            else if (s.toLowerCase().equals("string"))
                ts.add(Type.STRING_TYPE);
            else {
                System.out.println("Unknown type " + s);
                return;
            }
            }
            HeapFileEncoder.convert(new File(args[1]),
                        new File(args[1].replaceAll(".txt", ".dat")),
                        BufferPool.PAGE_SIZE,
                        Integer.parseInt(args[2]), ts.toArray(new Type[0]));

        } else {
            System.out.println("Unexpected number of arguments to convert ");
        }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        } else if (args[0].equals("print")) {
            File tableFile = new File(args[1]);
            int columns = Integer.parseInt(args[2]);
            DbFile table = Utility.openHeapFile(columns, tableFile);
            TransactionId tid = new TransactionId();
            DbFileIterator it = table.iterator(tid);
            
            if(null == it){
               System.out.println("Error: method HeapFile.iterator(TransactionId tid) not yet implemented!");
            } else {
               it.open();
               while (it.hasNext()) {
                  Tuple t = it.next();
                  System.out.println(t);
               }
               it.close();
            }
        }
        else {
            System.err.println("Unknown command: " + args[0]);
            System.exit(1);
        }
    }

}
