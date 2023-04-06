package top.wanjie.mysql.backend.vm;

import top.wanjie.mysql.backend.common.AbstractCache;
import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.dm.DataManager;
import top.wanjie.mysql.backend.tm.TransactionManager;
import top.wanjie.mysql.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/05/21:09
 */
public class VersionManagerImpl extends AbstractCache<Entry> implements VersionManager{
    TransactionManager tm;
    DataManager dm;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lt;

    public VersionManagerImpl(TransactionManager tm, DataManager dm) {
        super(0);
        this.tm = tm;
        this.dm = dm;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lt = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = null;
        try {
            t = activeTransaction.get(xid);
            if(t.err != null) throw t.err;
        } finally {
            lock.unlock();
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if(e == Error.NullEntryException) {
                return null;
            }else{
                throw e;
            }
        }

        try {
            if(Visibility.isVisible(tm, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dm.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if(t.err != null) throw t.err;
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch(Exception e) {
            if(e == Error.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try{
            if(!Visibility.isVisible(tm, t, entry)) {
                return false;
            }
            Lock l = null;
            try {
                lt.add(xid, uid);
            } catch (Exception e) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }
            if(l != null) {
                l.lock();
                l.unlock();
            }

            if(entry.getXmax() == xid) return false;

            if(Visibility.isVersionSkip(tm, t, entry)) {
                t.err = Error.ConcurrentUpdateException;
                internAbort(xid, true);
                t.autoAborted = true;
                throw t.err;
            }

            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = tm.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        try {
            Transaction t = activeTransaction.get(xid);
            if(t.err != null) throw t.err;
            activeTransaction.remove(xid);
        } finally {
            lock.unlock();
        }
        lt.remove(xid);
        tm.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid,false);
    }

    private void internAbort(long xid,boolean autoAborted) {
        lock.lock();
        try{
            Transaction t = activeTransaction.get(xid);
            if(!autoAborted) {
                activeTransaction.remove(xid);
            }
            if(t.autoAborted) return;
        } finally {
            lock.unlock();
        }

        lt.remove(xid);
        tm.abort(xid);

    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        //调用dm的get方法
        Entry entry = Entry.loadEntry(uid, this);
        if(entry == null) throw Error.NullEntryException;
        return entry;
    }


    @Override
    protected void releaseForCache(Entry entry) {
        //调用dm的release方法
        entry.remove();
    }
}
