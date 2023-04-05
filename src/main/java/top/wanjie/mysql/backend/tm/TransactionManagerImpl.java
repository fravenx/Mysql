package top.wanjie.mysql.backend.tm;

import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.utils.Panic;
import top.wanjie.mysql.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author fraven
 * @Description
 * @Date 2023/03/29/21:58
 */
public class TransactionManagerImpl implements TransactionManager{
    static final int XID_HEADER_LENGTH = 8;
    static final String XID_SUFFIX = ".xid";
    private static final int XID_FIELD_SIZE = 1;
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    public static final long SUPER_XID = 0;

    RandomAccessFile raf = null;
    FileChannel fc = null;
    private long xidCounter;
    private Lock counterLock;


    TransactionManagerImpl(RandomAccessFile raf,FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        checkXidCounter();
        this.counterLock = new ReentrantLock();
    }

    void checkXidCounter() {
        long length = 0;
        try {
            length = raf.length();
        } catch (IOException e) {
            Panic.panic(Error.BadXIDFileException);
        }

        ByteBuffer buf = ByteBuffer.wrap(new byte[8]);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        this.xidCounter = Parser.parseLong(buf.array());
        long logicLength = getXidPosition(this.xidCounter + 1);
        if(logicLength != length) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    private long getXidPosition(long xid) {
        return (xid - 1) * XID_FIELD_SIZE + XID_HEADER_LENGTH;
    }

    private void update(long xid,byte state) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[1];
        tmp[0] = state;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    private void increXidCounter(){
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }

    }

    @Override
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            update(xid,FIELD_TRAN_ACTIVE);
            increXidCounter();
            return xid;
        }finally {
            counterLock.unlock();
        }

    }

    @Override
    public void commit(long xid) {
        update(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public void abort(long xid) {
        update(xid,FIELD_TRAN_ABORTED);
    }

    private boolean checkXid(long xid,Byte state) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[1];
        tmp[0] = state;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == state;
    }

    @Override
    public boolean isActive(long xid) {
        if(xid == SUPER_XID) {
            return false;
        }
        return checkXid(xid,FIELD_TRAN_ACTIVE);
    }

    @Override
    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) {
            return true;
        }
        return checkXid(xid,FIELD_TRAN_COMMITTED);
    }

    @Override
    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) {
            return false;
        }
        return checkXid(xid,FIELD_TRAN_ABORTED);
    }

    @Override
    public void close() {
        try {
            fc.close();
            raf.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }
}
