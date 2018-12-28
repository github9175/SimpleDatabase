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

            int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;

            temp.seek(offset);

            byte[] data = new byte[BufferPool.PAGE_SIZE];

            temp.read(data, 0, BufferPool.PAGE_SIZE);

            temp.close();

            return new HeapPage((HeapPageId) pid, data);

        }catch(IOException e) {

            e.printStackTrace();

        }

        return null;
        
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        RandomAccessFile temp = new RandomAccessFile(file, "rw");

        PageId pid = page.getId();

        int offset = pid.pageNumber() * BufferPool.PAGE_SIZE;

        temp.seek(offset);

        temp.write(page.getPageData());

        temp.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.floor((file.length()/BufferPool.PAGE_SIZE));
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here

        ArrayList<Page> res = new ArrayList<Page>();

        HeapPage page = null;

        int i = 0;

        HeapPageId pid = null;
        
        while(i < numPages()) {

            pid = new HeapPageId(getId(), i);

            page = ((HeapPage) Database.getBufferPool().getPage(
                       tid, pid, Permissions.READ_WRITE));

            if (page.getNumEmptySlots() > 0) {

                break;

            }

            i++;
            
        }
        //no empty slots in previous pages, add a new page and write to disk
        if (i == numPages()) {

            pid = new HeapPageId(getId(), numPages());

            page  = new HeapPage(pid, HeapPage.createEmptyPageData());

            writePage(page);

            page = ((HeapPage) Database.getBufferPool().getPage(
                       tid, pid, Permissions.READ_WRITE));

        }

        page.insertTuple(t);

        page.markDirty(true, tid);

        res.add(page);

        return res;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        PageId pid = t.getRecordId().getPageId();

        HeapPage page = ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE));
       
        page.deleteTuple(t);

        page.markDirty(true, tid);
       
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private int pagePos;

        private Iterator<Tuple> currentTuple;

        private TransactionId tid;

        private boolean open;

        public HeapFileIterator(TransactionId tid) {

            this.tid = tid;

        }
        //tuples iterator in a page
        public Iterator<Tuple> pageIterator(HeapPageId pid) throws TransactionAbortedException, DbException{

            HeapPage page = (HeapPage) Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);

            return page.iterator();
        }

        @Override
        public Tuple next() throws TransactionAbortedException, DbException{

            if (!this.hasNext()) {

                throw new NoSuchElementException();

            }

            return currentTuple.next();
            
        }

        @Override
        public boolean hasNext() throws TransactionAbortedException, DbException{

            if (!open) {

                return false;
            }

            if (currentTuple.hasNext()){

                return true;

            }else{
                //turn to the next page
                pagePos++;

                while (pagePos < numPages()){

                    if (pageIterator(new HeapPageId(getId(), pagePos)).hasNext()){

                            currentTuple = pageIterator(new HeapPageId(getId(), pagePos));

                            return true;

                    }else{

                        pagePos++;

                    } 
                }

                return false;

            }
            
        }

        @Override
        public void open() throws TransactionAbortedException, DbException{

            open = true;

            pagePos = 0;
            
            HeapPageId pid = new HeapPageId(getId(), pagePos);

            currentTuple = pageIterator(pid);

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
            
            currentTuple = null;
        }
    }

}

