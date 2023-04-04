package top.wanjie.mysql.backend.dm;

import com.google.common.primitives.Bytes;
import top.wanjie.mysql.backend.common.SubArray;
import top.wanjie.mysql.backend.dm.dataItem.DataItem;
import top.wanjie.mysql.backend.dm.logger.Logger;
import top.wanjie.mysql.backend.dm.page.Page;
import top.wanjie.mysql.backend.dm.page.PageX;
import top.wanjie.mysql.backend.dm.pageCache.PageCache;
import top.wanjie.mysql.backend.tm.TransactionManager;
import top.wanjie.mysql.backend.utils.Parser;

import java.util.*;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/04/13:57
 */
// 在每条log（[Size] [Checksum] [Data]）的Data中
// updateLog:
// [LogType] [XID] [UID] [OldRaw] [NewRaw]
// 0         1     9
// insertLog:
// [LogType] [XID] [Pgno] [Offset] [Raw]
// 0         1     9      13       15
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;
    private static final int REDO = 0;
    private static final int UNDO = 1;
    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;
    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("recovering...");
        lg.rewind();
        int maxPgno = 0;
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            int pgno;
            if (isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgno = li.pgno;
            } else {
                UpdateLogInfo li = parseUpdateLog(log);
                pgno = li.pgno;
            }
            if (pgno > maxPgno) maxPgno = pgno;
        }
        if (maxPgno == 0) maxPgno = 1;
        pc.truncateByPageNo(maxPgno);
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc);
        System.out.println("Redo Transcations over");

        undoTranscations(tm, lg, pc);
        System.out.println("Undo Transcations over");

        System.out.println("Recover over");
    }

    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo ili = parseInsertLog(log);
                long xid = ili.xid;
                if (!tm.isActive(xid)) {
                    doInsertLog(pc, ili, REDO);
                }
            } else {
                UpdateLogInfo uli = parseUpdateLog(log);
                long xid = uli.xid;
                if (!tm.isActive(xid)) {
                    doUpdateLog(pc, uli, REDO);
                }
            }
        }
    }

    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> undoLogs = new HashMap<>();
        lg.rewind();
        while (true) {
            byte[] log = lg.next();
            if (log == null) break;
            long xid;
            if (isInsertLog(log)) {
                InsertLogInfo ili = parseInsertLog(log);
                xid = ili.xid;
            } else {
                UpdateLogInfo uli = parseUpdateLog(log);
                xid = uli.xid;
            }
            if (tm.isActive(xid)) {
                if (!undoLogs.containsKey(xid)) {
                    undoLogs.put(xid, new ArrayList<>());
                }
                undoLogs.get(xid).add(log);
            }
        }

        for (Map.Entry<Long, List<byte[]>> entry : undoLogs.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    InsertLogInfo ili = parseInsertLog(log);
                    doInsertLog(pc, ili, UNDO);
                } else {
                    UpdateLogInfo uli = parseUpdateLog(log);
                    doUpdateLog(pc, uli, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static void doInsertLog(PageCache pc, InsertLogInfo ili, int flag) {
        int pgno = ili.pgno;
        Page pg = pc.getPage(pgno);
        try {
            if (flag == UNDO) DataItem.setDataItemInValid(ili.raw);
            PageX.recoverInsert(pg, ili.offset, ili.raw);
        } finally {
            pg.release();
        }

    }

    private static void doUpdateLog(PageCache pc, UpdateLogInfo uli, int flag) {
        int pgno = uli.pgno;
        Page pg = pc.getPage(pgno);
        try {
            if (flag == REDO) {
                PageX.recoverUpdate(pg, uli.offset, uli.newRaw);
            } else {
                PageX.recoverUpdate(pg, uli.offset, uli.oldRaw);
            }

        } finally {
            pg.release();
        }
    }


    private static boolean isInsertLog(byte[] log) {
        return log[OF_TYPE] == LOG_TYPE_INSERT;
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo ili = new InsertLogInfo();
        ili.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        ili.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        ili.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        ili.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return ili;
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo uli = new UpdateLogInfo();
        uli.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        uli.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        uli.pgno = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        uli.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        uli.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + 2 * length);
        return uli;
    }

    public static byte[] wrapUpdateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    public static byte[] wrapInsertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }
}
