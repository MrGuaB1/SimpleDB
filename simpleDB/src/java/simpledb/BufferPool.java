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
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    private int numPages;

    private HashMap<PageId,Page> pages;

    private LockManager lockManager;

    /** Bytes per page, including header. */

    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

//******************************新增的数据结构*************************
    public class Lock{
        public TransactionId tid; //事务id
        public boolean isShared; //是不是共享锁

        public Lock(TransactionId tid,boolean isShared){
            this.tid = tid;
            this.isShared = isShared;
        }
    }

    public class LockManager{
        ConcurrentHashMap<PageId,ArrayList<BufferPool.Lock>> lockMap; //ConcurrentHashMap线程安全

        public LockManager(){
            lockMap = new ConcurrentHashMap<PageId,ArrayList<BufferPool.Lock>>();
        }

        public synchronized boolean acquireLock(PageId pid,TransactionId tid,boolean isShared) {
            //此页无锁则添加一个锁
            if (lockMap.get(pid) == null) {
                Lock lock = new Lock(tid, isShared);
                ArrayList<Lock> locks = new ArrayList<BufferPool.Lock>();
                locks.add(lock);
                lockMap.put(pid, locks);
                return true;
            }      
            //如果页面有锁，分为事务取得锁或事务未取得锁
            ArrayList lockList = lockMap.get(pid);
            for (Object o : lockList) { //遍历锁列表
                Lock lock = (Lock) o;
                if (lock.tid == tid) { //如果能找到tid相同的锁，证明该事务已经取得了当前页面的锁
                    if (lock.isShared == isShared) //两个锁一模一样，认为获取锁成功(锁重入)
                        return true;
                    
                    
                    
                    
                    if (!lock.isShared) //如果请求的锁是共享锁，而已经持有了独占锁，那么也认为获取成功
                        return true;
                    
                    
                    //原来拥有共享锁
                    if (lockList.size() == 1) { //到这里的逻辑是，tid原来拥有共享锁，现在要获取独占锁
                        lock.isShared = false;  //如果

                        
                    } 
                    else //否则此事务现在正在与其他事务共享一把锁，那么就不能升级为独占锁，认为获取锁失败
                    	return false;
                }
            }
            
            
            //事务在此之前并未获得过锁
            if (lockList.size() == 1 && !((Lock) lockList.get(0)).isShared) //tid不同，且为独占锁，获锁失败
                return false; 
            
            if (isShared) { //是共享锁，锁列表添加新锁
                    Lock lock = new Lock(tid, true);
                    lockList.add(lock);
                    lockMap.put(pid, lockList);
                    return true;
            }
            //页面只能获取共享锁，想获取独占锁判定失败
            return false;
        }
        
        
        
        

        public synchronized boolean releaseLock(PageId pid,TransactionId tid){
            if(lockMap.get(pid) == null)
                return false;            
            ArrayList<Lock> locks = lockMap.get(pid);
            for(int i=0;i<locks.size();++i){
                Lock lock = locks.get(i);
                if(lock.tid == tid){
                    locks.remove(lock);
                    if(locks.size() == 0) //页面上已经没有锁了，那么就把这个映射删除
                        lockMap.remove(pid);
                    return true;
                }
            }
            return false;
        }
        
        //此方法判断是否持有锁
        public synchronized boolean holdsLock(PageId pid,TransactionId tid){
            if(lockMap.get(pid) == null)
                return false;        
            ArrayList<Lock> locks =  lockMap.get(pid);
            for (int i = 0; i < locks.size(); i++) 
                if(locks.get(i).tid == tid)
                	return true;
            return false;
        }
    }
    //使用依赖图的思路：在获取锁之前，检查当前事务是否已经持有其他锁。如果当前事务持有其他锁，并且存在依赖关
    //系，需要中断其中一个锁的持有者，以打破死锁状态
    //在释放锁之前，更新依赖图，移除当前事务与其他锁之间的依赖关系
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
       this.numPages = numPages;
           pages = new HashMap<>();
           lockManager = new LockManager();
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
     * 
     * 检索具有关联权限的指定页面。将获取一个锁，如果该锁由另一个事务持有，则可能会阻塞。
     * 
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     * 
     * 应该在缓冲池中查找检索到的页面。 如果存在，则应将其退回。 
     * 如果不存在，则应将其添加到缓冲池并返回。 
     * 如果缓冲池中没有足够的空间，则应逐出一个页面并在其位置添加新页面。
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException{
        boolean isShared = (perm==Permissions.READ_ONLY ? true : false); //根据权限获取锁的类型
        boolean lockAcquired = false; //循环条件，判断是否成功取得了锁
        
        
        long start = System.currentTimeMillis();
        long limit = new Random().nextInt(2000) + 1000; //线程获得不同的值，分散并发操作的压力
        while(!lockAcquired){
            long now = System.currentTimeMillis();
            if(now-start > limit)
                throw new TransactionAbortedException();
            lockAcquired = lockManager.acquireLock(pid,tid,isShared);
        }
        
        if(tid == null)
        	throw new TransactionAbortedException();
        if (this.pages.containsKey(pid))
        	return pages.get(pid);
        if(pages.size()>= numPages)
        	evictPage();
        Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        pages.put(pid,page);
        return page;
    }
    
    private synchronized void evictPage() throws DbException {
        boolean allDirty = true;
        for(Map.Entry<PageId, Page> entry : pages.entrySet()) {
            if(entry.getValue().isDirty() != null) //如果是脏页就跳过
            	continue;
            try {
                allDirty = false;
                flushPage(entry.getKey()); //把非脏的页刷新到磁盘
            } catch (IOException e) {
                e.printStackTrace();
            }
            discardPage(entry.getKey()); //从缓冲池中删除这个页
            break;
        }
        if(allDirty)
        	throw new DbException("All pages are dirty");
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
    public void releasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        return lockManager.holdsLock(p,tid);
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
        if(commit) //如果要提交事务
        	flushPages(tid); //将tid对应的页面刷新到磁盘
        
        else{ //如果要终止事务，就要把页面恢复到它的磁盘状态来恢复事务所做的任何更改
            for (PageId pid : pages.keySet()) {
                Page page = pages.get(pid);
                if (page.isDirty() == tid) {
                	//获取页面的磁盘状态，即修改之前的状态
                    DbFile file =  Database.getCatalog().getDatabaseFile(pid.getTableId()); 
                    Page pageFromDisk = file.readPage(pid);
                    pages.put(pid, pageFromDisk); //把页面的磁盘状态重新put进缓冲池，实现事务回滚
                }
            }
        }
        
        //无论提交还是终止，都应该释放BufferPool中保持的关于事务的状态，即锁
        for(PageId pid:pages.keySet()){
            if(holdsLock(tid,pid))
                releasePage(tid,pid);
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
        ArrayList<Page> page = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page p:page) {
            p.markDirty(true,tid);
            pages.put(p.getId(),p);
        }
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
        ArrayList<Page> page = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page p:page) {
            p.markDirty(true,tid);
            pages.put(p.getId(),p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(Map.Entry<PageId, Page> entry : pages.entrySet()) 
            flushPage(entry.getKey());      
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        if(pages.get(pid).isDirty() != null)
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pages.get(pid));
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        for(Map.Entry<PageId, Page> entry : pages.entrySet()) {
            if(entry.getValue().isDirty() == tid)
            	flushPage(entry.getKey());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */

}