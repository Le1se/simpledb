package simpledb;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private ConcurrentHashMap<Field, Integer> fvMap;

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
        if (what != Aggregator.Op.COUNT) throw new
                IllegalArgumentException("Illegal Operation");

        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.fvMap = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (tup.getTupleDesc().getFieldType(this.gbfield).equals(this.gbfieldtype))
            this.fvMap.put(tup.getField(this.gbfield), this.fvMap.getOrDefault(tup.getField(this.gbfield),0)+1);

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
        //throw new UnsupportedOperationException("please implement me for lab3");
        TupleDesc td = new TupleDesc(new Type[] {this.gbfieldtype, Type.INT_TYPE});
        List<Tuple> TupleList = new ArrayList<>();
        Enumeration<Field> keys = this.fvMap.keys();
        while(keys.hasMoreElements()) {
            Tuple t  = new Tuple(td);
            Field f = keys.nextElement();
            t.setField(0, f);
            t.setField(1, new IntField(this.fvMap.get(f)));
            TupleList.add(t);
        }
        return new TupleIterator(td, TupleList);
    }

}
