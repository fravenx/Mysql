package top.wanjie.mysql.backend.tm;

import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @Author fraven
 * @Description
 * @Date 2023/03/26/15:32
 */
public interface TransactionManager {
    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();

    public static TransactionManagerImpl create(String path) {
        File file = new File(path + ".xid");
        try{
            if(!file.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        ByteBuffer buf = ByteBuffer.wrap(new byte[8]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        TransactionManagerImpl tm = new TransactionManagerImpl(raf, fc);
        return tm;

    }

    public static TransactionManagerImpl open(String path){
        File file = new File(path + ".xid");
        if(!file.exists()) {
            Panic.panic(Error.FileExistsException);
        }

        if(!file.canRead() || !file.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        TransactionManagerImpl tm = new TransactionManagerImpl(raf, fc);
        return tm;

    }
}
