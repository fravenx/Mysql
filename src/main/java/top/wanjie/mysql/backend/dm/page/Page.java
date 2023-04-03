package top.wanjie.mysql.backend.dm.page;

/**
 * @Author fraven
 * @Description
 * @Date 2023/04/01/15:29
 */
public interface Page {
    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();
}
