package top.wanjie.mysql.backend.dm.pageCache;

import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.dm.page.Page;
import top.wanjie.mysql.backend.dm.page.PageImpl;
import top.wanjie.mysql.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/03/12:52
 */
public interface PageCache {
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pageNo);
    void close();
    void release(Page pg);
    void truncateByPageNo(int maxPageNo);
    int getPageNumber();
    void flushPage(Page pg);

    public static PageCache create(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        try{
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        }catch (Exception e) {
            Panic.panic(e);
        }

        if(!f.canWrite() || !f.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory / PageImpl.PAGE_SIZE);
    }

    public static PageCache open(String path, long memory) {
        File f = new File(path + PageCacheImpl.DB_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }

        if(!f.canWrite() || !f.canRead()) {
            Panic.panic(Error.FileCannotRWException);
        }
        RandomAccessFile raf = null;
        FileChannel fc = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e){
            Panic.panic(e);
        }
        return new PageCacheImpl(raf, fc, (int)memory / PageImpl.PAGE_SIZE);
    }
}
