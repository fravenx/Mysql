package top.wanjie.mysql.backend.vm;

import top.wanjie.mysql.backend.common.Error;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/05/16:44
 */
public class LockTable {
    private Map<Long, List<Long>> x2u;
    private Map<Long, Long> u2x;
    private Map<Long, List<Long>> uwx;
    private Map<Long, Lock> waitLock;
    private Map<Long, Long> xwu;
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        uwx = new HashMap<>();
        waitLock = new HashMap<>();
        xwu = new HashMap<>();
        lock = new ReentrantLock();
    }

    //xid试图获取对uid的锁
    public Lock add(long xid, long uid) throws Exception{
        lock.lock();
        try{
            if(isInList(x2u, xid, uid)) {
                return null;
            }
            if(!u2x.containsKey(uid)) {
                putIntoList(x2u, xid, uid);
                u2x.put(uid, xid);
                return null;
            }
            xwu.put(xid, uid);
            putIntoList(uwx, uid, xid);
            if(hasDeadLock()) {
                xwu.remove(xid);
                removeFromList(uwx, uid, xid);
                throw Error.DeadlockException;
            }
            ReentrantLock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid ,l);
            return l;
        } finally {
            lock.unlock();
        }
    }

    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if(l != null) {
                while(l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXid(uid);
                }
            }
            xwu.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    private void selectNewXid(long uid) {
        u2x.remove(uid);
        List<Long> l = uwx.get(uid);
        if(l == null) return;
        assert l.size() > 0;

        while(l.size() > 0) {
            Long xid = l.remove(0);
            if(!waitLock.containsKey(xid)) {
                continue;
            }
            Lock lo = waitLock.remove(xid);
            lo.unlock();
            waitLock.remove(xid);
            putIntoList(x2u, xid, uid);
            u2x.put(uid, xid);
            xwu.remove(xid);
            break;
        }
        if(l.size() == 0) uwx.remove(uid);
    }

    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for (Long x : x2u.keySet()) {
            Integer s = xidStamp.get(x);
            if(s != null && s > 0) {
                continue;
            }
            stamp++;
            if(dfs(x)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer s = xidStamp.get(xid);
        if(s != null && s == stamp) return true;
        if(s != null && s < stamp) return false;
        xidStamp.put(xid, stamp);
        Long uid = xwu.get(xid);
        if(uid == null) return false;
        Long x = u2x.get(uid);
        assert x != null;
        return dfs(x);
    }


    private void removeFromList(Map<Long, List<Long>>map, long u0, long u1) {
        List<Long> l = map.get(u0);
        if(l == null) return;
        int index = 0;
        for (int i = 0; i < l.size(); i++) {
            if(l.get(i) == u1) {
                index = i;
            }
        }
        l.remove(index);
        if(l.size() == 0){
            map.remove(u0);
        }
    }

    private void putIntoList(Map<Long, List<Long>>map, long u0, long u1) {
        if(!map.containsKey(u0)) {
            map.put(u0,new ArrayList<>());
        }
        map.get(u0).add(u1);
    }

    private boolean isInList(Map<Long, List<Long>>map, long u0, long u1) {
        List<Long> l = map.get(u0);
        if(l == null) return false;
        for (Long x : l) {
            if(x == u1) return true;
        }
        return false;
    }


}
