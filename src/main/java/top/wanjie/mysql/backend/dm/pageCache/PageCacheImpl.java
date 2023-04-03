package top.wanjie.mysql.backend.dm.pageCache;

import top.wanjie.mysql.backend.common.AbstractCache;
import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.dm.page.Page;
import top.wanjie.mysql.backend.dm.page.PageImpl;
import top.wanjie.mysql.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/03/14:24
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache{
    private static final int MEM_MIN_LIM = 10;
    public static String DB_SUFFIX = ".db";
    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock fileLock;
    private AtomicInteger pageNumbers;

    PageCacheImpl(RandomAccessFile raf, FileChannel fc, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = raf.length();
        }catch (IOException e) {
            Panic.panic(e);
        }
        this.raf = raf;
        this.fc = fc;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PageImpl.PAGE_SIZE);

    }
    @Override
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        PageImpl page = new PageImpl(pgno, initData, null);
        flush(page);
        return pgno;
    }

    private void flush(Page page) {
        int pgno = page.getPageNumber();
        long pageOffset = pageOffset(pgno);
        fileLock.lock();
        try {
            fc.position(pageOffset);
            fc.write(ByteBuffer.wrap(page.getData()));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }

    private static long pageOffset(int pgno) {
        return (pgno - 1) * PageImpl.PAGE_SIZE;
    }

    @Override
    public Page getPage(int pageNo) {
        return get(pageNo);
    }


    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pgno = (int)key;
        long pageOffset = pageOffset(pgno);
        ByteBuffer buf = ByteBuffer.wrap(new byte[PageImpl.PAGE_SIZE]);
        fileLock.lock();
        try {
            fc.position(pageOffset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
        return new PageImpl(pgno, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    @Override
    public void release(Page pg) {
        release((long)pg.getPageNumber());
    }

    @Override
    public void truncateByPageNo(int maxPageNo) {
        long size = pageOffset(maxPageNo + 1);
        try {
            raf.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }
}
