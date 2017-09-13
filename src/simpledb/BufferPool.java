package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool which check that the transaction has the appropriate
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
    private int numValidPages;
    private int [] accTime;
    private Page [] page;
    private PageId [] pageId;
    private PageLock pl;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
    	this.numValidPages = 0;
    	this.accTime = new int[numPages];//-1 is empty.
    	for(int i = 0; i < numPages; i ++)
    		accTime[i] = -1;
    	page = new HeapPage[numPages];
    	pageId = new HeapPageId[numPages];   
    	pl = new PageLock();
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
    public synchronized Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
            // some code goes here
        	/*** Start -- Check the lock/permissions for TransactionId tid***/
        	
        	long TDeadLock = 150;
        	long stopTime = System.currentTimeMillis() + TDeadLock;
        	while(true){
        		if(perm == Permissions.READ_ONLY){
        			if(pl.readLockable(tid, pid)){
        				pl.lockPage(tid, pid, perm);
        				break;
        			}
        		} else {
        			if(pl.writeLockable(tid, pid)){
        				pl.lockPage(tid, pid, perm);
        				break;
        			}
        		}
        		long TimeOut = System.currentTimeMillis();
        		if(TimeOut >= stopTime)
        			throw new TransactionAbortedException();
        	}
        	/*** End -- Check the lock/permissions for TransactionId tid***/
        	/*---For lab1---*/
        	int tabID = ((HeapPageId)pid).getTableId();
        	HeapFile hpFile = (HeapFile)Database.getCatalog().getDbFile(tabID);
        	Page tempPage;
        	/*page No.pid is in Buffer already.*/
        	for(int i = 0; i < numPages; i++)	    		
        		if(((HeapPageId)pid).equals(pageId[i])){
        			accTime[i] ++;
        			return page[i];
        		}
        	
        	/*If BufferPool is full, evict one page. */
        	if(numPages == numValidPages){
        		evictPage();
        	}
        	/*Read the page from file to the BufferPool*/
        	tempPage = (HeapPage)hpFile.readPage(pid);
        	for(int k = 0 ; k < numPages ; k ++){
        		if(accTime[k] == -1){
        			page[k] = tempPage;
        			pageId[k] = (HeapPageId) pid;
        			accTime[k] = 1;
        			numValidPages ++;
        			break;
        		}
        	}
            return tempPage;
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
    public synchronized void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	pl.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    	pl.releaseLockTrans(tid);
    	for(int i = 0; i < numPages; i ++){
    		if(page[i] !=null && page[i].isDirty() != null && page[i].isDirty() == tid )
    			flushPage(pageId[i]);
    	}
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public  synchronized boolean holdsLock(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    	return (pl.lockLevel(tid, pid) != -1);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public  synchronized void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
            // some code goes here
            // not necessary for lab1|lab2
        	pl.releaseLockTrans(tid);
        	if(commit){
        		for(int i = 0; i < numPages; i ++)
            		if(page[i] !=null && page[i].isDirty() != null && page[i].isDirty() == tid )
            			flushPage(pageId[i]);    		
        	}else{
        		for(int i = 0; i < numPages; i ++)
            		if(page[i] !=null && page[i].isDirty() != null && page[i].isDirty() == tid ){
            			//re-read the page
        				HeapFile tempFile = (HeapFile)Database.getCatalog().getDbFile(pageId[i].getTableId());
        				HeapPage tmpPage = (HeapPage) tempFile.readPage(pageId[i]);
        				page[i] = tmpPage;
            		} 
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
    public synchronized void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
            // some code goes here
            // not necessary for lab1
        	/*
        	HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(tableId);
        	hf.addTuple(tid, t);  
        	*/
        	int i;
        	HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(tableId);
        	int pageNum = hf.numPages();
        	for(i = 0; i < pageNum; i++){
        		HeapPageId hpID = new HeapPageId(tableId, i);
        		HeapPage hp = (HeapPage)Database.getBufferPool().getPage(tid, hpID, Permissions.READ_WRITE);
        		if(hp.getNumEmptySlots() > 0){
        			//System.out.println("Before Insert on page No."+i);
        	    	//hp.printTuplesInPage();
        			hp.addTuple(t);
        			hp.markDirty(true, tid);
        			//hf.writePage(hp);
        			//System.out.println("After Insert:");
        	    	//hp.printTuplesInPage();
        			break;
        		}   		
        	}
        	// if all pages are full.
        	if(i == pageNum){
        		hf.addEmptyPage(tid);
        		HeapPageId hpId = new HeapPageId(tableId, i);
        		HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, hpId, Permissions.READ_WRITE);
        		hp.addTuple(t);
        		hp.markDirty(true, tid);
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
    public synchronized void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	RecordId rid = t.getRecordId();
    	HeapPageId dtPageId = (HeapPageId)rid.getPageId();
    	HeapPage dpage = (HeapPage) Database.getBufferPool().getPage(tid, dtPageId, Permissions.READ_WRITE);
    	dpage.deleteTuple(t);
    	dpage.markDirty(true, tid);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	for(int i = 0; i < numPages; i++){
    		flushPage(pageId[i]);
    	}
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab4
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private  synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // some code goes here
        // not necessary for lab1
    	if(pid != null){
    		int tableId = pid.getTableId();
        	for(int i = 0; i < numPages; i++)
        		if(pageId[i] != null && pageId[i].equals(pid)){
        			HeapFile hf = (HeapFile)Database.getCatalog().getDbFile(tableId);
        	    	hf.writePage(page[i]);
        		}
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
     private  synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	int minLoc = -1;
    	int minAccT = 9999;
    	int itx;
    	//find the page with the least access times
    	for(itx = 0 ; itx < numPages ; itx ++){
    		//find a page with the least access times and not write locked by a uncommitted transaction 
    		if(accTime[itx] < minAccT){
    			if(page[itx].isDirty() != null && pl.writeLocked(pageId[itx]))
    				continue;
    			else {
	    			minLoc = itx;
	    			minAccT = accTime[itx];
    			}
    		}
    	}
    	if(minLoc == -1)
    		throw new DbException("No proper pages to evict");
    	
    	//find a page.
    	//If this page is dirty and not locked by a uncommitted transaction, flush to disk.
    	if(page[minLoc].isDirty() != null){
    		try {
				flushPage(pageId[minLoc]);
			} catch (IOException e) {
				e.printStackTrace();
			}
    	}
    	accTime[minLoc] = -1;
    	numValidPages --;
    }
    

}
