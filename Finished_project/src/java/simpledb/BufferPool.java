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

    private volatile int numPages;

    private volatile LRUCache cache;

    private volatile LockManager lockManager;
    
    private volatile Map<TransactionId, Long> transactions;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {

        this.numPages = numPages;
  
        cache = new LRUCache(numPages);

        lockManager = new LockManager();
    
        transactions = new ConcurrentHashMap<TransactionId, Long>();

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

        if(tid != null){
            
            if (!transactions.containsKey(tid)) {

                long t = System.currentTimeMillis();

                transactions.put(tid, t);

            }
                //acquire locks and detect deadlocks.
                boolean res = lockManager.acquireLock(pid, tid, perm);

                while(!res){

                    if ((System.currentTimeMillis() - transactions.get(tid)) > 500) {

                        throw new TransactionAbortedException();
                    }

                    try {

                        Thread.sleep(10);

                        res = lockManager.acquireLock(pid, tid, perm);

                    } catch (InterruptedException e){

                        e.printStackTrace();

                    }
                }
            
        }

        //get pages from LRU
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
        lockManager.releasePage(pid, tid);
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
 
        transactions.remove(tid);

        if(commit == true){

            flushPages(tid);

        }else{

            try{
                //restore pages in caches
                if(lockManager.tdReadLocks.get(tid) != null){

                    for (PageId pid : lockManager.tdReadLocks.get(tid)){

                            if(cache.access(pid) != null){

                                cache.put(pid, cache.access(pid).getBeforeImage());

                            }

                    }
                }

                if(lockManager.tdWriteLocks.get(tid) != null){

                    for (PageId pid : lockManager.tdWriteLocks.get(tid)){

                        if(cache.access(pid) != null){

                            cache.put(pid, cache.access(pid).getBeforeImage());

                        }

                    }
                }

            }catch(DbException e){

                e.printStackTrace();

            }
        }
        //release all related locks
        lockManager.releaseTid(tid);
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
            Database.getCatalog().getDbFile(tableId).insertTuple(tid, t);
        //marked and cached in heapfile.insertTuuple
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

        f.deleteTuple(tid, t);

        //marked and cached in heapfile.deleteTuple

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

            fPage.setBeforeImage();

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

                cache.ca.remove(cur.pid);

                cache.remove(cur);

                break;

            } 

            cur = cache.getNext(cur);

        }
        
        if(cur == null)  throw new DbException("Cannot be evicted since all pages in bufferpool are dirty");
    }

    public class LRUCache {
    //a linked list of pages
    private volatile Map<PageId, Node> ca; //pageId to pages
    private volatile Node head;
    private volatile Node tail;
    private volatile int c; //capacity

    public LRUCache(int capacity) {

        this.c = capacity;

        this.ca = new ConcurrentHashMap<PageId, Node>();

    }
    
    public Node getHead() {

        return this.head;

    }

    public Node getNext(Node node) {

        return node.next;

    }

    public Page get(PageId pid) throws DbException {//get from cache or from disk, move it to the tail

        Node node = this.ca.get(pid);

        if(node != null){

            remove(node);

            addToTail(node);

            return node.page;

        }else{

            HeapFile table = (HeapFile) Database.getCatalog().getDbFile(pid.getTableId());

            HeapPage newPage = (HeapPage) table.readPage(pid);

            this.put(pid, newPage);

            return newPage;

        }
        
    }

    public Page access(PageId pid) {//get pages

        Node node = this.ca.get(pid);

        if(node != null){

            return node.page;

        }else{

            return null;

        }
    }
       
    
    public void put(PageId pid, Page page) throws DbException {//put pages to the tail and evict undirty pages if it is full.

        Node node = new Node(pid, page);

        if(this.ca.containsKey(pid)){

            remove(this.ca.get(pid));

        } else if(this.ca.size() >= c){

            evictPage();

        }

        addToTail(node);

        ca.put(pid,node);
    }
    
    public void remove(Node node){//removes page node from the linked list

        if(node == null) return;

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

public class Node {//linked list node of pages
        private volatile Node prev;
        private volatile Node next;
        private volatile PageId pid;
        private volatile Page page;

        public Node(PageId pid, Page page){
            this.pid = pid;
            this.page = page;
        }

    }
private class LockManager {//read and write locks of pages to tid, tid to pages

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

        
        public boolean holdsLock(TransactionId tid, PageId pid){

            return (pageReadLocks.get(pid).contains(tid) || pageWriteLocks.get(pid).equals(tid));
        }

        
        public synchronized void releasePage(PageId pid, TransactionId tid){

            if (pageReadLocks.containsKey(pid)){

                pageReadLocks.get(pid).remove(tid);
            }

            pageWriteLocks.remove(pid);

            if (tdReadLocks.containsKey(tid)){

                tdReadLocks.get(tid).remove(pid);
            }

            if (tdWriteLocks.containsKey(tid)) {

                tdWriteLocks.get(tid).remove(pid);
            }

        }
        
        public synchronized void releaseTid(TransactionId tid){

            for (PageId pid : pageWriteLocks.keySet()) {

                if (pageWriteLocks.containsKey(pid) && pageWriteLocks.get(pid).equals(tid)) {

                     pageWriteLocks.remove(pid);

                }

            }
        
            for (PageId pid : pageReadLocks.keySet()) {

                if (pageReadLocks.containsKey(pid)) {

                    pageReadLocks.get(pid).remove(tid);

                }
            }

            tdReadLocks.remove(tid);

            tdWriteLocks.remove(tid);

            
    }

        public synchronized boolean acquireLock(PageId pid, TransactionId tid, Permissions perm){

            if (perm.equals(Permissions.READ_ONLY)){// read

                if ( !pageWriteLocks.containsKey(pid) || pageWriteLocks.get(pid).equals(tid)) {//not have other write block, add read lock

                    if (!pageReadLocks.containsKey(pid)){

                        pageReadLocks.put(pid, new HashSet<TransactionId>());

                    }

                    pageReadLocks.get(pid).add(tid);

                    if (!tdReadLocks.containsKey(tid)) {

                        tdReadLocks.put(tid, new HashSet<PageId>());

                    }

                    tdReadLocks.get(tid).add(pid);

                    return true;

                } else {

                    return false;//have other write block

                }

            } else {

                if (pageReadLocks.containsKey(pid)){

                    Set<TransactionId> set = pageReadLocks.get(pid);
                    //has another read lock
                    if(set.size() > 1 || (set.size() == 1 && !set.contains(tid)))

                        return false;

                }

                if (pageWriteLocks.containsKey(pid) && !pageWriteLocks.get(pid).equals(tid)){//has another write lock

                    return false;

                } else {//add write lock

                    pageWriteLocks.put(pid, tid);

                    if (!tdWriteLocks.containsKey(tid)){

                        tdWriteLocks.put(tid, new HashSet<PageId>());

                    }

                    tdWriteLocks.get(tid).add(pid);

                    return true;

                }
            }
        }
    }
}
