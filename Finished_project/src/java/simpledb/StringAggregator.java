package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op what;

    private Map<Field, Integer> count;

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (what != Op.COUNT) {

            throw new IllegalArgumentException("Invalid operator");

        }

        this.gbfield = gbfield;

        this.gbfieldtype = gbfieldtype;

        this.afield = afield;

        this.what = what;

        if(what == Op.COUNT) count = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field f;

        if(gbfield == Aggregator.NO_GROUPING){

            f = new IntField(0);

        }else{
            
            f = tup.getField(gbfield);
        }

        count.put(f, count.getOrDefault(f, 0) + 1);

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
        List<Tuple> tuples = new ArrayList<>();

        TupleDesc td;

        if(gbfield == Aggregator.NO_GROUPING){

            td = new TupleDesc(new Type[] {Type.INT_TYPE});

        }else{

            td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }

        for( Field f : count.keySet()){

            Tuple t = new Tuple(td);

            if (gbfield == Aggregator.NO_GROUPING) {

                t.setField(0, new IntField(count.get(f)));

            } else {
                
                t.setField(0, f);

                t.setField(1, new IntField(count.get(f)));
            }

            tuples.add(t);
        }

        return new TupleIterator(td, tuples);
    }

}
