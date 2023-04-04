package top.wanjie.mysql.backend.dm.dataItem;

import top.wanjie.mysql.backend.common.SubArray;
import top.wanjie.mysql.backend.dm.DataManager;
import top.wanjie.mysql.backend.dm.DataManagerImpl;
import top.wanjie.mysql.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/03/21:12
 */
public class DataItemImpl implements DataItem{
    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rlock;
    private Lock wlock;
    private DataManagerImpl dm;

    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, long uid, Page page, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        this.uid = uid;
        this.page = page;
        this.dm = dm;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        this.rlock = lock.readLock();
        this.wlock = lock.writeLock();
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte)0;
    }
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wlock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wlock.unlock();
    }

    @Override
    public void after(long xid) {
        dm.logDataItem(xid ,this);
        wlock.unlock();
    }

    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wlock.lock();
    }

    @Override
    public void unlock() {
        wlock.unlock();
    }

    @Override
    public void rLock() {
        rlock.lock();
    }

    @Override
    public void rUnLock() {
        rlock.unlock();
    }

    @Override
    public Page page() {
        return page;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
}
