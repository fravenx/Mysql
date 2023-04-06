package top.wanjie.mysql.backend.vm;

import java.util.Map;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/05/15:56
 */
public class Transaction {
    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        if(level != 0) {
            for (Long x: active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    public boolean isInSnapshot(long xid) {
        return snapshot.containsKey(xid);
    }

}
