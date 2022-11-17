package simpledb;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    //field to value map
    private ConcurrentHashMap<Field, Integer> fvMap;
    //field to number map
    private ConcurrentHashMap<Field, Integer> fnMap;

    private static final Field NO_GROUPING_FIELD = new IntField(0);

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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        fvMap = new ConcurrentHashMap<>();
        fnMap = new ConcurrentHashMap<>();

        if (this.gbfield == Aggregator.NO_GROUPING) {
            this.fvMap.put(NO_GROUPING_FIELD, 0);
            this.fnMap.put(NO_GROUPING_FIELD, 0);
        }
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
        Field key = NO_GROUPING_FIELD;
        if (gbfield != Aggregator.NO_GROUPING) key = tup.getField(this.gbfield);

        int value = ((IntField) (tup.getField(this.afield))).getValue();
        if (this.gbfield == Aggregator.NO_GROUPING ||
                tup.getTupleDesc().getFieldType(this.gbfield).equals(this.gbfieldtype)) {
            if (!(this.fvMap.containsKey(key))) {
                this.fvMap.put(key, value);
                this.fnMap.put(key, 1);
            } else {
                int newValue=0;
                if(what.equals(Op.MIN)){
                    newValue=Math.min(this.fvMap.get(key), value);
                }else if(what.equals(Op.MAX)){
                    newValue=Math.max(this.fvMap.get(key), value);
                }else if(what.equals(Op.SUM)) {
                    newValue=this.fvMap.get(key)+ value;
                }else if(what.equals(Op.AVG)){
                    newValue=this.fvMap.get(key)+ value;
                }else if(what.equals(Op.COUNT)){
                    newValue=this.fvMap.get(key)+ 1;
                }

                this.fvMap.put(key,newValue);
                this.fnMap.put(key,this.fnMap.get(key) + 1);
            }
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
        TupleDesc td;
        Tuple t;
        List<Tuple> tupleList = new ArrayList<>();
        if (this.gbfield == Aggregator.NO_GROUPING) {
            td = new TupleDesc(new Type[] { Type.INT_TYPE });
            t = new Tuple(td);
            int value = this.fvMap.get(NO_GROUPING_FIELD);
            if (this.what == Aggregator.Op.AVG) value /= this.fnMap.get(NO_GROUPING_FIELD);
            else if (this.what == Aggregator.Op.COUNT) value = this.fnMap.get(NO_GROUPING_FIELD);
            t.setField(0, new IntField(value));
            tupleList.add(t);
        } else {
            td = new TupleDesc(new Type[] { this.gbfieldtype, Type.INT_TYPE });
            Enumeration<Field> keys = this.fvMap.keys();
            while (keys.hasMoreElements()) {
                t = new Tuple(td);
                Field key = keys.nextElement();
                int value = this.fvMap.get(key);
                if (this.what == Aggregator.Op.AVG) value /= this.fnMap.get(key);
                else if (this.what == Aggregator.Op.COUNT) value = this.fnMap.get(key);
                t.setField(0, key);
                t.setField(1, new IntField(value));
                tupleList.add(t);
            }
        }

        return new TupleIterator(td, tupleList);
    }

}
