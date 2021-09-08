package simpledb;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionId;

import java.io.File;

/**
 * @Xinhao
 * @Date 2021/9/3
 * @Descrption
 */
public class Test {
    public static void main(String[] args) {
        Type[] types = {Type.INT_TYPE,Type.INT_TYPE,Type.INT_TYPE,Type.INT_TYPE};
        String[] fields = {"Field1","Field2","Field3","Field4"};
        TupleDesc tupleDesc = new TupleDesc(types, fields);

        HeapFile table1 = new HeapFile(new File("file.dat"), tupleDesc);
        Database.getCatalog().addTable(table1, "test");
        TransactionId tid = new TransactionId();
        int tableId = table1.getId();

        SeqScan seqScan = new SeqScan(tid,tableId);

        try {
            seqScan.open();
            while (seqScan.hasNext()){

                Tuple tp = seqScan.next();
                System.out.println(tp);
            }

        }catch (Exception e){
            e.printStackTrace();
        }

    }






}
