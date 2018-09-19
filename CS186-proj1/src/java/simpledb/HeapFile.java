package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

     private TupleDesc td;

     private File file;

     private int numPage;
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.td = td;

        this.file = f;

        numPage = (int)(file.length() / BufferPool.PAGE_SIZE);
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // some code goes here
        try (RandomAccessFile temp = new RandomAccessFile(file, "r")){

        int pos = pid.pageNumber() * BufferPool.PAGE_SIZE;

        temp.seek(pos);

        byte[] data = new byte[BufferPool.PAGE_SIZE];

        temp.read(data);

        return new HeapPage((HeapPageId) pid, data);

     }catch (IOException e) {

            e.printStackTrace();

        }
        return null;
        
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        RandomAccessFile temp = new RandomAccessFile(file, "rw");

        PageId pid = page.getId();

        int pos = pid.pageNumber() * BufferPool.PAGE_SIZE;

        temp.seek(pos);

        temp.write(page.getPageData());

        temp.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return numPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for proj1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private int pagePos;

        private Iterator<Tuple> tuple;

        private TransactionId tid;

        private boolean open;

        public HeapFileIterator(TransactionId tid) {

            this.tid = tid;

        }

        public Iterator<Tuple> pageIterator(HeapPageId pid) throws TransactionAbortedException, DbException{

            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);

            return page.iterator();
        }

        @Override
        public Tuple next() throws TransactionAbortedException, DbException{

            if (!open) {
                throw new DbException("The file is not opened.");
            }

            if (!hasNext()) {
                throw new DbException("No more tuples.");
            }


                if (!tuple.hasNext()) {

                    pagePos++;

                    tuple = pageIterator(new HeapPageId(getId(), pagePos));

                }

                return tuple.next();

            
            
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException{

            if (!open) {
                return false;
            }

            if (tuple.hasNext()){

                return true;

            }
            
            if (pagePos < numPages() && pageIterator(new HeapPageId(getId(), pagePos + 1)).hasNext()){
                
                return true;
            } 

            return false;
        
        }

        @Override
        public void open() throws TransactionAbortedException, DbException{

            open = true;

            pagePos = 1;
            
            HeapPageId pid = new HeapPageId(getId(), pagePos);

            tuple = pageIterator(pid);
        }

        @Override
        public void rewind()throws TransactionAbortedException, DbException{

            if (!open) {

               throw new  DbException("The file is not opened.");
            }

            close();

            open();
        }

        @Override
        public void close() {

            open = false;

            pagePos = 0;
            
            tuple = null;
        }
    }

}

