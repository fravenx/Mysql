package top.wanjie.mysql.backend.dm.logger;

import com.google.common.primitives.Bytes;
import top.wanjie.mysql.backend.common.Error;
import top.wanjie.mysql.backend.utils.Panic;
import top.wanjie.mysql.backend.utils.Parser;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author fraven
 * @Description
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 * XChecksum 为后续所有日志计算的Checksum，int类型
 * 每条正确日志的格式为：
 * [Size] [Checksum] [Data]
 * Size 4字节int 标识Data长度
 * Checksum 4字节int
 * @Date 2023/04/03/17:46
 */
public class LoggerImpl implements Logger{
    private static final int SEED = 13331;

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4;
    private static final int OF_DATA = OF_CHECKSUM + 4;

    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile raf;
    private FileChannel fc;
    private Lock lock;

    private long position;
    private long fileSize;
    private int xCheckSum;

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.raf = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xCheckSum) {
        this.raf = raf;
        this.fc = fc;
        this.lock = new ReentrantLock();
        this.xCheckSum = xCheckSum;
    }

    void init() {
        long size = 0;
        try {
            size = raf.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xCheckSum = Parser.parseInt(buf.array());
        this.fileSize = size;
        checkAndRemoveTail();
    }

    private void checkAndRemoveTail() {
        rewind();
        int xCheck = 0;
        while(true) {
            byte[] log = internNext();
            if(log == null) break;
            xCheck = calCheckSum(xCheck,log);
        }
        if(xCheck != xCheckSum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);
            raf.seek(position);
        } catch (Exception e) {
            Panic.panic(e);
        }
        rewind();

    }

    private byte[] internNext() {
        if(position + OF_DATA >= fileSize) {
            return null;
        }
        ByteBuffer buf = ByteBuffer.allocate(4);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(buf.array());
        if(size + OF_DATA + position > fileSize) {
            return null;
        }
        buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();
        int checkSum1 = Parser.parseInt(Arrays.copyOfRange(log,OF_CHECKSUM,OF_DATA));
        int checkSum2 = calCheckSum(0, Arrays.copyOfRange(log,OF_DATA,log.length));
        if(checkSum1 != checkSum2) {
            return null;
        }
        position += log.length;
        return log;


    }

    private int calCheckSum(int checkSum,byte[] data) {
        for (byte b : data) {
            checkSum = checkSum * SEED + b;
        }
        return checkSum;
    }

    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);
        lock.lock();
        try {
            fc.position(fc.size());
            fc.write(ByteBuffer.wrap(log));
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXCheckSum(log);
    }

    private void updateXCheckSum(byte[] log) {
        xCheckSum = calCheckSum(xCheckSum, log);
        lock.lock();
        try {
            fc.position(0);
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xCheckSum)));
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private byte[] wrapLog(byte[] data) {
        int length = data.length;
        int checkSum = calCheckSum(0, data);
        return Bytes.concat(Parser.int2Byte(length), Parser.int2Byte(checkSum), data);
    }
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            fc.truncate(x);
        }finally {
            lock.unlock();
        }
    }

    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();
            if(log == null) {
                return null;
            }
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void rewind() {
        position = 4;
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
