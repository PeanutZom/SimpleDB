package simpledb.storage;

import simpledb.common.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    File f;
    TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    @Override
    public int getId() {
        // some code goes here
        return f.getAbsolutePath().hashCode();
        //throw new UnsupportedOperationException("implement this");
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
        //throw new UnsupportedOperationException("implement this");
    }

    // see DbFile.java for javadocs
    @Override
    public Page readPage(PageId pid) {
        // some code goes here
        int offset = pid.getPageNumber()*BufferPool.getPageSize();
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            RandomAccessFile rfile = new RandomAccessFile(f,"r");
            rfile.seek(offset);
            rfile.read(data);
            rfile.close();
            return new HeapPage((HeapPageId) pid,data);
        }catch (Exception e){
            e.printStackTrace();
        }

        return null;
    }

    // see DbFile.java for javadocs
    @Override
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    @Override
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    @Override
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            int pageNo = -1;
            PageId currPageID;
            Iterator<Tuple> tupleIterator;
            int tableID;

            @Override

            public void open() throws DbException, TransactionAbortedException {
                pageNo = 0;
                tableID = getId();
                currPageID = new HeapPageId(tableID,0);
                System.out.println(((HeapPage)Database.getBufferPool().getPage(tid,currPageID,Permissions.READ_ONLY)));
                tupleIterator = ((HeapPage)Database.getBufferPool().getPage(tid,currPageID,Permissions.READ_ONLY)).iterator();

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (tupleIterator == null){
                    return false;
                }
                boolean hasNextInPage = tupleIterator.hasNext();
                if (hasNextInPage){
                    return true;
                }else{
                    if (pageNo+1<numPages()){
                        currPageID = new HeapPageId(tableID,++pageNo);
                        tupleIterator = ((HeapPage)Database.getBufferPool().getPage(tid,currPageID,Permissions.READ_ONLY)).iterator();
                        return tupleIterator.hasNext();
                    }else {
                        return false;
                    }
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                try {
                    return tupleIterator.next();
                }catch (NoSuchElementException e){
                    if (pageNo+1<numPages()){
                        currPageID = new HeapPageId(tableID,++pageNo);
                        tupleIterator = ((HeapPage)Database.getBufferPool().getPage(tid,currPageID,Permissions.READ_ONLY)).iterator();
                        return tupleIterator.next();
                    }else {
                        throw new NoSuchElementException();
                    }
                }catch (NullPointerException e){
                    throw new NoSuchElementException();
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                currPageID = null;
                tupleIterator = null;
                pageNo = -1;
            }
        };
    }

}

