package top.wanjie.mysql.backend.dm.pageIndex;

import top.wanjie.mysql.backend.dm.page.PageImpl;
import top.wanjie.mysql.backend.dm.pageCache.PageCacheImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @Author fraven
 * @Description 帮助挑选一个合适的页面进行插入，统计每个页面的剩余空闲大小，将每个页面大小分为40份
 * @Date 2023/04/03/16:43
 */
public class PageIndex {
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageImpl.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex(){
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for(int i = 0;i < INTERVALS_NO + 1;i++) {
            lists[i] = new ArrayList<>();
        }
    }

    public void add(int pageNo,int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            number = Math.min(INTERVALS_NO,number);
            lists[number].add(new PageInfo(pageNo,freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int num = spaceSize / THRESHOLD;
            if(num < INTERVALS_NO) num++;
            while(num <= INTERVALS_NO) {
                if(lists[num].size() == 0) {
                    num++;
                    continue;
                }
                return lists[num].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }
}
