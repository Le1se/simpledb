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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc td;
    private int tableId;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.td=td;
        this.tableId=file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        //throw new UnsupportedOperationException("implement this");
        return this.tableId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        //throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
            byte[] buffer = new byte[BufferPool.getPageSize()];
            raf.seek((long) pid.pageNumber() * BufferPool.getPageSize());
            raf.read(buffer);
            return new HeapPage((HeapPageId) pid, buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException("Read page failed with given pid");
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length()/BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }
    class HeapFileIterator extends AbstractDbFileIterator {

        Iterator<Tuple> iter;
        int pageNum;
        TransactionId tid;
        HeapFile heapFile;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }

        public void open() throws DbException, TransactionAbortedException {
            this.pageNum = -1;
        }

        @Override
        protected Tuple readNext() throws TransactionAbortedException, DbException {
            if (iter != null && !iter.hasNext())
                iter = null;
            while (iter == null && pageNum < heapFile.numPages() - 1) {
                HeapPageId currId = new HeapPageId(heapFile.getId(), pageNum);
                HeapPage currPage = (HeapPage) Database.getBufferPool().getPage(tid, currId, Permissions.READ_ONLY);
                iter = currPage.iterator();
                if (!iter.hasNext()) iter = null;
                pageNum++;
            }
            if (iter == null) return null;
            return iter.next();
        }

        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        public void close() {
            super.close();
            iter = null;
            pageNum = Integer.MAX_VALUE;
        }
    }

}

