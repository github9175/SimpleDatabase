package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages;

    private LRUCache cache;

    private volatile LockManager lockManager;
    
    private volatile Map<TransactionId, Long> allTransactions;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {

        this.numPages = numPages;
  
        cache = new LRUCache(numPages);

        lockManager = new LockManager();
    
        allTransactions = new ConcurrentHashMap<TransactionId, Long>();

    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        if (!allTransactions.containsKey(tid)) {

            long t0 = System.currentTimeMillis();

            allTransactions.put(tid, t0);
            
            boolean granted = lockManager.grantLock(pid, tid, perm);

            while( !granted){

                if ((System.currentTimeMillis() - allTransactions.get(tid)) > 250) {

                    throw new TransactionAbortedException();
                }
                try {

                    Thread.sleep(200);

                    granted = lockManager.grantLock(pid, tid, perm);

                } catch (InterruptedException e){

                    e.printStackTrace();

                }
            }

        } else {

            boolean granted = lockManager.grantLock(pid, tid, perm);

            while( !granted){

                if ((System.currentTimeMillis() - allTransactions.get(tid)) > 500) {

                    throw new TransactionAbortedException();
                }

                try {

                    Thread.sleep(10);

                    granted = lockManager.grantLock(pid, tid, perm);

                } catch (InterruptedException e){

                    e.printStackTrace();

                }
            }
        }


        return cache.get(pid);
            
    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj1
        lockManager.releaseLock(pid, tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj1
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj1
 
        allTransactions.remove(tid);

        if(commit == true){

            flushPages(tid);

        }else{

        }
        for (PageId pid : lockManager.tdReadLocks.get(tid)){

            if(commit == false){

                cache.put(pid, cache.ca.get(pid).page.getBeforeImage());

            }

            releasePage(tid, pid);
        }
        for (PageId pid : lockManager.tdWriteLocks.get(tid)){

            if(commit == false){
                
                cache.put(pid, cache.ca.get(pid).page.getBeforeImage());

            }

            releasePage(tid, pid);
        }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        for (Page p: Database.getCatalog().getDbFile(tableId).insertTuple(tid, t)) {
            
            p.markDirty(true, tid);
            

        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        int tableId=t.getRecordId().getPageId().getTableId();

        HeapFile f = (HeapFile) Database.getCatalog().getDbFile(tableId);

        Page p = f.deleteTuple(tid, t);

        p.markDirty(true,tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        Node cur = cache.getHead();

        while(cur != null){

            flushPage(cur.pid);

            cur = cache.getNext(cur);

        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        HeapPage fPage = (HeapPage) cache.access(pid);
        
        if(fPage == null) return;

        if(fPage.isDirty() != null){

            HeapFile table = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());

            fPage.markDirty(false, null);

            table.writePage(fPage);

        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        Node cur = cache.getHead();

        while(cur != null){

            if(cur.page.isDirty() == tid) flushPage(cur.pid);

            cur = cache.getNext(cur);

        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        Node cur = cache.getHead();

        while(cur != null){

            if(cur.page.isDirty() == null) {

                cache.remove(cur);

                break;

            } 

            cur = cache.getNext(cur);

        }
        
        if(cur == null)  throw new DbException("Cannot be evicted since all pages in bufferpool are dirty");
    }

    public class LRUCache {

    Map<PageId, Node> ca;
    Node head;
    Node tail;
    int c;

    public LRUCache(int capacity) {

        c = capacity;

        ca = new HashMap<PageId, Node>();

    }
    
    public Node getHead() {

        return head;

    }

    public Node getNext(Node node) {

        return node.next;

    }

    public Page get(PageId pid) {

        Node node = ca.get(pid);

        if(node != null){

            remove(node);

            addToTail(node);

            return node.page;

        }else{

            HeapFile table = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());

            HeapPage newPage = (HeapPage) table.readPage(pid);

            put(pid, newPage);

            return newPage;

        }
        
    }

    public Page access(PageId pid) {

        Node node = ca.get(pid);

        if(node != null){

            return node.page;

        }else{

            return null;

        }
    }
       
    
    public void put(PageId pid, Page page) {

        Node node = new Node(pid, page);

        if(ca.containsKey(pid)){

            remove(ca.get(pid));

        } else if(ca.size() >= c){

            ca.remove(head.pid);

            remove(head);

        }

        addToTail(node);

        ca.put(pid,node);
    }
    
    public void remove(Node node){

        if(node == null) return;

        Page f = node.page;

        try {

            flushPage(node.pid);

        }catch(IOException e) {

            e.printStackTrace();

        }

        if(node.prev!=null){

            node.prev.next = node.next;

        } else {

            head = node.next;

        }

        if(node.next!=null){

            node.next.prev = node.prev;

        } else {

            tail = node.prev;

        }
    }

    
    public void addToTail(Node node){

        if(tail == null){

            head = node;

            tail = node;

        } else {

            tail.next = node;

            node.prev = tail;

            node.next = null;

            tail = node;
        }
    }
    
    
}

public class Node {
        Node prev;
        Node next;
        PageId pid;
        Page page;
        public Node(PageId pid, Page page){
            this.pid = pid;
            this.page = page;
        }

    }
private class LockManager {

        private Map<PageId, Set<TransactionId>> pageReadLocks;
        private Map<PageId, TransactionId> pageWriteLocks;
        private Map<TransactionId, Set<PageId>> tdReadLocks;
        private Map<TransactionId, Set<PageId>> tdWriteLocks;

        
        public LockManager(){
            pageReadLocks = new ConcurrentHashMap<PageId, Set<TransactionId>>();
            pageWriteLocks = new ConcurrentHashMap<PageId, TransactionId>();
            tdReadLocks = new ConcurrentHashMap<TransactionId, Set<PageId>>();
            tdWriteLocks = new ConcurrentHashMap<TransactionId, Set<PageId>>();
        }

        /**
         * check to see if a transaction has a lock on a page
         * @param  tid specified TransactionID
         * @param  pid specified PageId
         * @return     true if tid has a lock on pid
         */
        public boolean holdsLock(TransactionId tid, PageId pid){
            Set<TransactionId> readtid;
            TransactionId writetid; 
            readtid = pageReadLocks.get(pid);
            writetid = pageWriteLocks.get(pid);
            return (readtid.contains(tid) || writetid.equals(tid));
        }

        /**
         * release a transaction's lock on a page specified by pid
         * @param pid pageId of this page
         * @param tid TransactionId of this transaction
         */
        public synchronized void releaseLock(PageId pid, TransactionId tid){
            Set<PageId> pidReadSet = tdReadLocks.get(tid);
            Set<PageId> pidWriteSet = tdWriteLocks.get(tid);
            Set<TransactionId> tidReadSet = pageReadLocks.get(pid);
            if (tidReadSet!= null){
                tidReadSet.remove(tid);
                pageReadLocks.put(pid, tidReadSet);
            }
            pageWriteLocks.remove(pid);
            if (pidReadSet != null){
                pidReadSet.remove(pid);
                tdReadLocks.put(tid, pidReadSet);
            }
            if (pidWriteSet != null) {
                pidWriteSet.remove(pid);
                tdWriteLocks.put(tid, pidWriteSet);
            }
        }
        
        public synchronized void releaseAllTidLocks(TransactionId tid){
            for (PageId pageId : pageWriteLocks.keySet()) {
                if (pageWriteLocks.get(pageId) != null && pageWriteLocks.get(pageId)==tid) {
                     pageWriteLocks.remove(pageId);
                }
            }
            
        
            for (PageId pageId : pageReadLocks.keySet()) {
                Set<TransactionId> tidSet = pageReadLocks.get(pageId);
                if (tidSet != null) {
                    tidSet.remove(tid);
                    pageReadLocks.put(pageId, tidSet);

                }
            }
            tdReadLocks.remove(tid);
            tdWriteLocks.remove(tid);

            
    }

        public synchronized boolean grantLock(PageId pid, TransactionId tid, Permissions perm){

            if (perm.equals(Permissions.READ_ONLY)){// read
                Set<TransactionId> tidReadSet = pageReadLocks.get(pid);
                TransactionId writetid = pageWriteLocks.get(pid);
                if (writetid == null || writetid.equals(tid)) {//not have other write block, add read lock

                    if (tidReadSet == null){
                        tidReadSet = new HashSet<TransactionId>();
                    }

                    tidReadSet.add(tid);
                    pageReadLocks.put(pid, tidReadSet);


                    Set<PageId> pageReadSet = tdReadLocks.get(tid);
                    if (pageReadSet == null) {
                        pageReadSet = new HashSet<PageId>();
                    }
                    pageReadSet.add(pid);
                    tdReadLocks.put(tid, pageReadSet);
                    return true;

                } else {
                    return false;//have other write block
                }

            } else {
                Set<TransactionId> tidReadSet = pageReadLocks.get(pid);
                TransactionId writetid = pageWriteLocks.get(pid);

                if (tidReadSet != null && tidReadSet.size() > 1){//has another read lock
                    return false;
                }
                if (tidReadSet != null && tidReadSet.size() == 1 && !tidReadSet.contains(tid)){//has another read lock
                    return false;
                }
                if (writetid != null && !writetid.equals(tid)){//has another write lock
                    return false;
                } else {//add write lock
                    pageWriteLocks.put(pid, tid);
                    Set<PageId> pidWriteSet = tdWriteLocks.get(tid);
                    if (pidWriteSet == null){
                        pidWriteSet = new HashSet<PageId>();
                    }
                    pidWriteSet.add(pid);
                    tdWriteLocks.put(tid, pidWriteSet);
                    return true;

                }
            }
        }
    }


}
