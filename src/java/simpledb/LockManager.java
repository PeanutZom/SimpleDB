package simpledb;

import simpledb.storage.Page;
import simpledb.storage.PageId;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Xinhao
 * @Date 2021/10/17
 * @Descrption
 */
public class LockManager {
    public enum LockType{
        SLock, XLock
    }

    class ObjLock{
        LockType type;
        Set<TransactionId> holders;
        PageId pid;

        public ObjLock(LockType type, PageId pid){
            this.type = type;
            this.pid = pid;
            holders = Collections.newSetFromMap(new ConcurrentHashMap<>());
        }

        public void addHolder(TransactionId tid){
            holders.add(tid);
        }

        public Set<TransactionId> getHolders() {
            return holders;
        }

        public LockType getType() {
            return type;
        }

        public void setType(LockType type) {
            this.type = type;
        }

    }

    private ConcurrentHashMap<PageId, ObjLock> lockTable;
    private ConcurrentHashMap<TransactionId,ArrayList<PageId>> transactionTable;

    public LockManager(){
        lockTable = new ConcurrentHashMap<>();
        transactionTable = new ConcurrentHashMap<>();
    }

    public synchronized void acquireLock(TransactionId tid, PageId pid, LockType type)throws TransactionAbortedException{

            if (holdsLock(tid, pid)){
                //If the transaction holds the lock already
                long startTime = System.currentTimeMillis();
                while (true){
                    if (type==LockType.XLock&&lockTable.get(pid).getType()==LockType.SLock){
                        if (lockTable.get(pid).holders.size()==1){
                            upgradeLock(pid);
                        }else {
                            block(tid,startTime);
                            continue;
                        }
                    }
                    return;
                }

            }
            long startTime = System.currentTimeMillis();
            while (true) {
                if (lockTable.containsKey(pid)) {
                    //Condition where there is an existing lock.
                    ObjLock lock = lockTable.get(pid);
                    if (lock.getType() == LockType.SLock && type == LockType.SLock) {
                        lock.addHolder(tid);
                        updateTransactionTable(tid, pid);
                        break;
                    } else {// Wait until other transactions release locks.
                        block(tid,startTime);
                    }
                } else {//Condition where there is no lock on the page
                    ObjLock toAdd = new ObjLock(type, pid);
                    toAdd.addHolder(tid);
                    lockTable.put(pid, toAdd);
                    updateTransactionTable(tid, pid);
                    break;
                }
            }

    }

    public void updateTransactionTable(TransactionId tid, PageId pid){
        if (transactionTable.containsKey(tid)){
            transactionTable.get(tid).add(pid);
        }else {
            ArrayList<PageId> toAdd = new ArrayList<>();
            toAdd.add(pid);
            transactionTable.put(tid,toAdd);
        }
    }

    public void upgradeLock(PageId pid){
        lockTable.get(pid).setType(LockType.XLock);
    }

    public synchronized void block(TransactionId tid, long start) throws TransactionAbortedException{
        try {
            wait(100);
            if (System.currentTimeMillis()-start > 1000){
                releaseTransaction(tid);
                throw new TransactionAbortedException();
            }
        }catch (InterruptedException e){
            e.printStackTrace();
        }

    }

    public synchronized void releaseLock(TransactionId tid, PageId pid){
        ObjLock toRelease = lockTable.get(pid);
        Set<TransactionId> holders = toRelease.getHolders();
        if (holders.contains(tid)){
            holders.remove(tid);
            transactionTable.get(tid).remove(pid);
        }
        if (holders.isEmpty()){
            lockTable.remove(pid);
            transactionTable.get(tid).remove(pid);
            if (transactionTable.get(tid).size()==0){
                transactionTable.remove(tid);
            }
            notifyAll();
        }

    }

    public synchronized void releaseTransaction(TransactionId tid){
        if (transactionTable.containsKey(tid)){
            ArrayList<PageId> pages = transactionTable.get(tid);
            for (int i=0;i<pages.size();i++){
                releaseLock(tid, pages.get(i));
            }
        }else {
            return;
        }
    }

    public boolean holdsLock(TransactionId tid, PageId pid){
        if (lockTable.containsKey(pid)){
        return lockTable.get(pid).getHolders().contains(tid);
        }
        return false;
    }

}
