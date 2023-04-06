package top.wanjie.mysql.backend.vm;

import top.wanjie.mysql.backend.tm.TransactionManager;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/05/16:12
 */
public class Visibility {

    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        if(t.level == 0) {
            return false;
        }

        if(tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax))) return true;
        return false;
    }

    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else{
            return repeatableRead(tm, t, e);
        }
    }

    //(XMIN == Ti and                             // 由Ti创建且
    //    XMAX == NULL                            // 还未被删除
    //)
    //or                                          // 或
    //(XMIN is commited and                       // 由一个已提交的事务创建且
    //    (XMAX == NULL or                        // 尚未删除或
    //    (XMAX != Ti and XMAX is not commited)   // 由一个未提交的事务删除
    //))
    public static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin)) {
            if(xmax == 0 || xmax != xid && !tm.isCommitted(xmax)) return true;
        }

        return false;
    }

    //(XMIN == Ti and                 // 由Ti创建且
    // (XMAX == NULL or               // 尚未被删除
    //))
    //or                              // 或
    //(XMIN is commited and           // 由一个已提交的事务创建且
    // XMIN < XID and                 // 这个事务小于Ti且
    // XMIN is not in SP(Ti) and      // 这个事务在Ti开始前提交且
    // (XMAX == NULL or               // 尚未被删除或
    //  (XMAX != Ti and               // 由其他事务删除且
    //   (XMAX is not commited or     // 这个事务尚未提交或
    //XMAX > Ti or                    // 这个事务在Ti开始之后才开始或
    //XMAX is in SP(Ti)               // 这个事务在Ti开始时还活跃
    //))))
    public static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;
        long xmin = e.getXmin();
        long xmax = e.getXmax();

        if(xmin == xid && xmax == 0) return true;

        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            if(xmax == 0 || xmax != xid && (!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax))) {
                return true;
            }
        }
        return false;
    }

}
