package top.wanjie.mysql.backend.common;

import top.wanjie.mysql.backend.utils.Panic;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * AbstractCache 实现了一个引用计数策略的缓存
 */
public abstract class AbstractCache<T> {
    private HashMap<Long,T> cache;
    private HashMap<Long,Integer> references;
    private HashMap<Long,Boolean> getting;
    private Integer maxSize;
    private Integer count;
    private Lock lock;

    public AbstractCache(int _maxSize){
        maxSize       =_maxSize;
        cache         = new HashMap<>();
        references    = new HashMap<>();
        getting       = new HashMap<>();
        lock          = new ReentrantLock();
        count         = 0;
    }

    protected T get(long key) {
        while(true) {
            lock.lock();
            if(getting.containsKey(key)){
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                continue;
            }

            if(cache.containsKey(key)) {
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1);
                lock.unlock();
                return obj;
            }

            if(maxSize > 0 && count == maxSize) {
                Panic.panic(Error.CacheFullException);
            }

            getting.put(key,true);
            lock.unlock();
            break;
        }

        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            getting.remove(key);
            lock.unlock();
            Panic.panic(e);
        }

        lock.lock();
        getting.remove(key);
        cache.put(key,obj);
        references.put(key,1);
        count++;
        lock.unlock();

        return obj;
    }

    protected void release(long key) {
        lock.lock();
        try {
            Integer refCount = references.get(key);
            refCount--;
            if(refCount == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                cache.remove(key);
                references.remove(key);
                count--;
            }else{
                references.put(key,refCount);
            }
        }finally {
            lock.unlock();
        }
    }

    protected void close() {
        lock.lock();
        Set<Long> keys = cache.keySet();
        for (Long key : keys) {
            T obj = cache.get(key);
            releaseForCache(obj);
            cache.remove(key);
            references.remove(key);
        }

    }

    protected abstract T getForCache(long key) throws Exception;

    protected abstract void releaseForCache(T obj);



 }