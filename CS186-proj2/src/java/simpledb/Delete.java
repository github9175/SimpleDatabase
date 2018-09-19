package simpledb;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    private TransactionId t;

    private DbIterator child;

    private boolean ifDelete;

    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.t = t;

        this.child = child;

        ifDelete = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (ifDelete == true) {

            return null;

        }
        
        int count = 0;
        
        BufferPool bufferP = Database.getBufferPool();

        while (child.hasNext()) {

            Tuple tu = child.next();

            bufferP.deleteTuple(t, tu);

            count++;
        }

        ifDelete = true;

        TupleDesc temptd = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[]{null});

        Tuple res = new Tuple(temptd);

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
