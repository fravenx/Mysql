package top.wanjie.mysql.backend.vm;

import com.google.common.primitives.Bytes;
import top.wanjie.mysql.backend.common.SubArray;
import top.wanjie.mysql.backend.dm.dataItem.DataItem;
import top.wanjie.mysql.backend.utils.Parser;

import javax.xml.crypto.Data;
import java.sql.DataTruncation;
import java.util.Arrays;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/05/15:15
 */

// entry结构：
// [XMIN] [XMAX] [data]
public class Entry {
   private static final int OF_XMIN = 0;
   private static final int OF_XMAX = 8;
   private static final int OF_DATA = 16;

   private long uid;
   private DataItem di;
   private VersionManager vm;

   public static Entry newEntry(long uid, DataItem di, VersionManager vm) {
       Entry entry = new Entry();
       entry.di = di;
       entry.uid = uid;
       entry.vm = vm;
       return entry;
   }

   public static Entry loadEntry(long uid, VersionManager vm) throws Exception {
       DataItem di = ((VersionManagerImpl)vm).dm.read(uid);
       if(di == null) return null;
       return newEntry(uid, di ,vm);
   }

   public static byte[] wrapEntryRaw(long xid, byte[] data) {
       byte[] xMin = Parser.long2Byte(xid);
       byte[] xMax = new byte[8];
       return Bytes.concat(xMin, xMax, data);
   }

   public void release() {
       ((VersionManagerImpl)vm).releaseEntry(this);
   }

   public void remove() {
       di.release();
   }

    // 返回entry中除去xmin，xmax的data部分，以深拷贝的方式
    public byte[] data() {
       di.rLock();
       try {
           SubArray raw = di.getRaw();
           byte[] data = new byte[raw.end - raw.start - OF_DATA];
           System.arraycopy(raw, raw.start + OF_DATA, data, 0, data.length);
           return data;
       } finally {
         di.rUnLock();
       }
   }

    public long getXmin() {
        di.rLock();
        try {
            SubArray sa = di.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMIN, sa.start+OF_XMAX));
        } finally {
            di.rUnLock();
        }
    }

    public long getXmax() {
        di.rLock();
        try {
            SubArray sa = di.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start+OF_XMAX, sa.start+OF_DATA));
        } finally {
            di.rUnLock();
        }
    }

    public void setXmax(long xid) {
        di.before();
        try {
            SubArray sa = di.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start+OF_XMAX, 8);
        } finally {
            di.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }
}
