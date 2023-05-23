
package simpledb;

import java.util.*;
import java.io.*;

/**
 * The interface for database files on disk. Each table is represented by a
 * single DbFile. DbFiles can fetch pages and iterate through tuples. Each
 * file has a unique id used to store metadata about the table in the Catalog.
 * DbFiles are generally accessed through the buffer pool, rather than directly
 * by operators.
 */
//磁盘上数据库文件的接口。 每个表都由一个 DbFile 表示。
//DbFiles 可以获取页面并遍历元组。 每个文件都有一个唯一的 ID，用于在目录中存储有关表的元数据。
//DbFiles 一般通过缓冲池访问，而不是直接由操作员访问。
public interface DbFile {
    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId id);

    /**
     * Push the specified page to disk. 将指定页面推送到磁盘
     *
     * @param p The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    //page.getId().pageno()指定应写入页面的文件中的偏移量
    public void writePage(Page p) throws IOException;

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     * 
     *代表事务向文件插入指定的元组此方法将在文件的受影响页面上获取锁，并可能阻塞直到获取锁
     *
     * @param tid The transaction performing the update  执行更新的事务
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file. 这个元组应该更新以反映它现在存储在这个文件中
     * @return An ArrayList contain the pages that were modified 返回被修改的页的数组
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     *代表指定的事务从文件中删除指定的元组。
     *此方法将在文件的受影响页面上获取锁，并且可能会阻塞，直到可以获取锁为止。
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException;

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     *返回存储在此 DbFile 中的所有元组的迭代器。 
     *迭代器必须使用 {@link BufferPool#getPage}，而不是 {@link #readPage} 来遍历页面。
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    public DbFileIterator iterator(TransactionId tid);

    /**
     * Returns a unique ID used to identify this DbFile in the Catalog. This id
     * can be used to look up the table via {@link Catalog#getDatabaseFile} and
     * {@link Catalog#getTupleDesc}.
     * <p>
     * 迭代器必须使用 {@link BufferPool#getPage}，而不是 {@link #readPage} 来遍历页面。
     * 
     * Implementation note:  you will need to generate this tableid somewhere,
     * ensure that each HeapFile has a "unique id," and that you always
     * return the same value for a particular HeapFile. A simple implementation
     * is to use the hash code of the absolute path of the file underlying
     * the HeapFile, i.e. <code>f.getAbsoluteFile().hashCode()</code>.
     *
     *实施注意事项：您需要在某处生成此 tableid，确保每个 HeapFile 都有一个“唯一 ID”，
     *并且您始终为特定的 HeapFile 返回相同的值。 
     *一个简单的实现是使用 HeapFile 底层文件的绝对路径的哈希码，即 f.getAbsoluteFile().hashCode()
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId();
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc();
}
