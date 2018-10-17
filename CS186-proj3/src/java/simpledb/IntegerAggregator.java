package simpledb;

import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    private int gbfield;

    private Type gbfieldtype;

    private int afield;

    private Op what;

    private Map<Field, int[]> avg;

    private Map<Field, Integer> count;

    private Map<Field, Integer> max;

    private Map<Field, Integer> min;

    private Map<Field, Integer> sum;

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;

        this.gbfieldtype = gbfieldtype;

        this.afield = afield;

        this.what = what;

        if(what == Op.AVG) avg = new HashMap<Field, int[]>();

        if(what == Op.COUNT) count = new HashMap<Field, Integer>();

        if(what == Op.MAX) max = new HashMap<Field, Integer>();

        if(what == Op.MIN) min = new HashMap<Field, Integer>();

        if(what == Op.SUM) sum = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if(what == Op.AVG) {

            Field f;

            if(gbfield == Aggregator.NO_GROUPING){

                f = new IntField(0);

            }else{

                f = tup.getField(gbfield);
            }

            avg.putIfAbsent(f, new int[] {0, 0});

            int average = avg.get(f)[0];

            int num = avg.get(f)[1];

            average = average * num + ((IntField) tup.getField(afield)).getValue();

            num++; 

            average /= num;

            avg.put(f, new int[] {average, num});
            

        }

        if(what == Op.COUNT) {

            Field f;

            if(gbfield == Aggregator.NO_GROUPING){

                f = new IntField(0);

            }else{

                f = tup.getField(gbfield);
            }

            count.putIfAbsent(f, 0);

            int c = count.get(f);

            c++;

            count.put(f, c);

        }

        if(what == Op.MAX) {

            Field f;

            if(gbfield == Aggregator.NO_GROUPING){

                f = new IntField(0);

            }else{

                f = tup.getField(gbfield);
            }

            int m = ((IntField) tup.getField(afield)).getValue();

            max.putIfAbsent(f, m);

            if(max.get(f) < m) max.put(f, m);
            
        }

        if(what == Op.MIN) {

            Field f;

            if(gbfield == Aggregator.NO_GROUPING){

                f = new IntField(0);

            }else{

                f = tup.getField(gbfield);
            }

            int m = ((IntField) tup.getField(afield)).getValue();

            min.putIfAbsent(f, m);

            if(min.get(f) > m) max.put(f, m);
            
        }

        if(what == Op.SUM) {

            Field f;

            if(gbfield == Aggregator.NO_GROUPING){

                f = new IntField(0);

            }else{

                f = tup.getField(gbfield);
            }

            sum.putIfAbsent(f, 0);

            sum.put(f, sum.get(f) + ((IntField) tup.getField(afield)).getValue());
            
        }
        
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();

        TupleDesc td;

        if(gbfield == Aggregator.NO_GROUPING){

            td = new TupleDesc(new Type[] {Type.INT_TYPE});

        }else{

            td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
        }

        if(what != Op.AVG) {

            Map<Field, Integer> map = new HashMap<Field, Integer>();

            if(what == Op.COUNT) map = count;

            if(what == Op.MAX) map = max;

            if(what == Op.MIN) map = min;

            if(what == Op.SUM) map = sum;

            Tuple t = new Tuple(td);

            for( Field f : map.keySet()){

                if (gbfield == Aggregator.NO_GROUPING) {

                t.setField(0, new IntField(map.get(f)));

            } else {
                t.setField(0, f);

                t.setField(1, new IntField(map.get(f)));
            }

            tuples.add(t);

            }

        }else{

            Tuple t = new Tuple(td);

            for( Field f : avg.keySet()){

                if (gbfield == Aggregator.NO_GROUPING) {

                t.setField(0, new IntField(avg.get(f)[0]));

            } else {
                
                t.setField(0, f);

                t.setField(1, new IntField(avg.get(f)[0]));
            }

            tuples.add(t);

            }


        }

        return new TupleIterator(td, tuples);
    }

}
