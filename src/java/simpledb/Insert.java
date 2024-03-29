package simpledb;
import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private Tuple tuple = null;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        if (!(Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc()))) {
            throw new DbException("TupleDesc from table and of child are different");
        }
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.tuple=new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"number"}));
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tuple.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        //return null;
        if (tuple.getField(0) != null) {
            return null;
        }
        int num = 0;
        try {
            while (child.hasNext()) {
                Tuple tuple = child.next();
                Database.getBufferPool().insertTuple(t, tableId, tuple);
                num++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        tuple.setField(0, new IntField(num));

        return tuple;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        this.child=children[0];
    }
}
