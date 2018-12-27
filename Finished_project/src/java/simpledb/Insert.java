package simpledb;

import java.util.*;

import java.io.IOException;
/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean insertonce;
    private TupleDesc td;

    public Insert(TransactionId t, DbIterator child, int tableid)
            throws DbException {
        // some code goes here
            this.t = t;

            this.child = child;

            this.tableid = tableid;

            this.td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[]{null});

            insertonce = false;

            if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableid))) {

            throw new DbException("TupleDesc of child differs from table");

        }

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();

        child.open();
    }

    public void close() {
        // some code goes here
        child.close();

        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
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
        if (insertonce == true) {

            return null;

        }
        
        int count = 0;
        
        BufferPool bufferPool = Database.getBufferPool();

        while (child.hasNext()) {

            Tuple tuple = child.next();

            try{

                bufferPool.insertTuple(t, tableid, tuple);

            }catch(IOException e){

                e.printStackTrace();
            }
            
            count++;
        }

        insertonce = true;

        Tuple res = new Tuple(td);

        res.setField(0, new IntField(count));

        return res;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
