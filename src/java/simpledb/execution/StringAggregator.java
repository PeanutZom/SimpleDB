package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private boolean grouping;

    private TupleDesc td;

    private Map<Field,List<Tuple>> groupMap = new HashMap<>();
    private List<Tuple> noGroupList = new ArrayList<>();

    private Map<Field,Integer> resultGroup = new HashMap<>();
    private Integer singleResult;

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

        if (gbfieldtype==null){
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
            grouping = false;
        }else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            grouping = true;
        }

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (grouping){
            Field gb = tup.getField(gbfield);
            Tuple toMerge = new Tuple(td);
            toMerge.setField(0, tup.getField(gbfield));
            toMerge.setField(1, tup.getField(afield));

            if (groupMap.containsKey(gb)){
                groupMap.get(gb).add(toMerge);
            }else {
                ArrayList<Tuple> newList = new ArrayList<>();
                newList.add(toMerge);
                groupMap.put(gb,newList);
            }
            groupHandler(toMerge);
        }else {
            Tuple toMerge = new Tuple(td);
            toMerge.setField(0, tup.getField(afield));
            noGroupList.add(toMerge);
            noGroupHandler(toMerge);
        }
    }

    private void noGroupHandler(Tuple tup){
        String value = ((StringField)tup.getField(afield)).getValue();
        switch (what){

            case COUNT: if(singleResult==null){
                singleResult = 1;
            }else {
                singleResult ++;
            }
        }
    }

    private void groupHandler(Tuple tuple){
        Field key = tuple.getField(gbfield);
        String value = ((StringField)tuple.getField(afield)).getValue();

        if (!resultGroup.containsKey(key)){
            resultGroup.put(key,null);

        }

        Integer exist = resultGroup.get(key);

        switch (what){

            case COUNT:if (exist==null){
                resultGroup.put(key,1);
            }else {
                resultGroup.put(key,exist+1);
            }break;
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    @Override
    public OpIterator iterator() {
        // some code goes here
        //throw new UnsupportedOperationException("please implement me for lab2");
        if (!grouping){
            List<Tuple> singleTup = new ArrayList<>();
            Tuple resultTup = new Tuple(td);
            resultTup.setField(0, new IntField(singleResult));
            singleTup.add(resultTup);
            return new TupleIterator(td,singleTup);
        }

        return new OpIterator() {
            Iterator<Field> keyIterator;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                keyIterator = resultGroup.keySet().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {

                return keyIterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                Tuple resultTup = new Tuple(td);
                Field gbField = keyIterator.next();
                Integer aggregateVal = resultGroup.get(gbField);
                resultTup.setField(0,gbField);
                resultTup.setField(1,new IntField(aggregateVal));
                return resultTup;
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                keyIterator = null;
            }

        };
    }

}
