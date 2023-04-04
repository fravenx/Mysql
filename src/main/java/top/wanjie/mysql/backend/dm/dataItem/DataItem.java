package top.wanjie.mysql.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import top.wanjie.mysql.backend.common.SubArray;
import top.wanjie.mysql.backend.dm.DataManagerImpl;
import top.wanjie.mysql.backend.dm.page.Page;
import top.wanjie.mysql.backend.utils.Parser;
import top.wanjie.mysql.backend.utils.Types;

import java.util.Arrays;

/**
 * @Author fraven
 * @Description [ValidFlag] [DataSize] [Data]
 * @Date 2023/04/03/21:12
 */
public interface DataItem {
    SubArray data();
    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();
    SubArray getRaw();

    public static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] dataSize = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, dataSize, raw);
    }

    public static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] pgData = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(pgData, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(pg.getPageNumber(), offset);
        SubArray raw = new SubArray(pgData, offset, offset + length);
        byte[] oldRaw = new byte[length];
        return new DataItemImpl(raw, oldRaw, uid, pg, dm);
    }

    public static void setDataItemInValid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }



}
