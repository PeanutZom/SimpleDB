package simpledb.storage;

import simpledb.LockManager;
import simpledb.common.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import java.io.*;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    LockManager lockManager = new LockManager();
    Page[] pool;
    boolean[] clock;
    int numPages;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pool = new Page[numPages];
        clock = new boolean[numPages];
        this.numPages = numPages;
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        LockManager.LockType type;
        if (perm == Permissions.READ_ONLY){
            type = LockManager.LockType.SLock;
        }else {
            type = LockManager.LockType.XLock;
        }
        lockManager.acquireLock(tid, pid, type);
        for (int i=0;i<pool.length;i++){
            if (pool[i]!=null&&pool[i].getId().equals(pid)){
                clock[i] = true;
                return pool[i];
            }
        }

        for (int i=0;i<pool.length;i++){
            if (pool[i]==null){
                pool[i] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                clock[i] = true;
                return pool[i];
            }
        }
        evictPage();
        for (int i=0;i<pool.length;i++){
            if (pool[i]==null){
                pool[i] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                clock[i] = true;
                return pool[i];
            }
        }

        // Page not found. release the lock
        lockManager.releaseLock(tid, pid);
        return null;
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
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid){
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit){
        // some code goes here
        // not necessary for lab1|lab2
        if (commit){
            for (int i = 0; i<pool.length;i++){
                if (pool[i]!=null&&pool[i].isDirty()!=null&&pool[i].isDirty().equals(tid)){
                    flushPage(pool[i].getId());
                }
            }
            lockManager.releaseTransaction(tid);
        }else {
            for (int i = 0; i<pool.length;i++){
                if (pool[i]!=null&&pool[i].isDirty()!=null&&pool[i].isDirty().equals(tid)){
                    PageId pid = pool[i].getId();
                    pool[i] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                }
            }
            lockManager.releaseTransaction(tid);
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int toDeleteTableId = t.getRecordId().getPageId().getTableId();
        Database.getCatalog().getDatabaseFile(toDeleteTableId).deleteTuple(tid, t);

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages(){
        // some code goes here
        // not necessary for lab1
        try {
            for (Page p : pool){
                if (p==null){
                    continue;
                }
                if (p.isDirty()!=null){
                    Database.getCatalog().getDatabaseFile(p.getId().getTableId()).writePage(p);
                    p.markDirty(false, null);
                }
            }
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        for (int i =0; i< pool.length; i++){
            if (pool[i].getId().equals(pid)){
                pool[i] = null;
                return;
            }
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid){
        // some code goes here
        // not necessary for lab1
        try {
            Page toFlush = null;
            for (Page p : pool){
                if (p!=null&&p.getId().equals(pid)){
                    toFlush = p;
                }
            }
            Database.getCatalog().getDatabaseFile(((HeapPageId)pid).tableId).writePage(toFlush);
            toFlush.markDirty(false, null);
        }catch (IOException e){
            e.printStackTrace();
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2

    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        int tick = 0;
        int dirtyPages = 0;

            // Skip the page if it was accessed in the last run or it is dirty.
            while (clock[tick%numPages]||pool[tick%numPages].isDirty()!=null){
                clock[tick%numPages] = false;
                if (pool[tick%numPages].isDirty()!=null){
                    dirtyPages++;
                    if (dirtyPages>numPages){
                        throw new DbException("Full of dirty pages");
                    }
                }
                tick++;
            }
            flushPage(pool[tick%numPages].getId());
            discardPage(pool[tick%numPages].getId());

    }

}
