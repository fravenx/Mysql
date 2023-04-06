package top.wanjie.mysql.backend.vm;

import top.wanjie.mysql.backend.dm.DataManager;
import top.wanjie.mysql.backend.tm.TransactionManager;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/05/15:21
 */
public interface VersionManager {
    byte[] read(long xid, long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);
    void commit(long xid) throws Exception;
    void abort(long xid);

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
